import { Codes } from '../codes.js';
// Generic stream creator
// Params: { name, natsContext, diagnostics, configuration }
// - name: string stream name
// - natsContext: created via createNatsContext
// - diagnostics: diagnostics instance
// - configuration: JetStream stream config (e.g., { subjects: [...], retention, ... })
export async function createStream({ name, natsContext, diagnostics, configuration = {} }) {
  const jsm = await natsContext.jetstreamManager();

  try {
    await jsm.streams.add({ name, ...configuration });
  } catch (err) {
    await reportCreateStreamError({ err, name, configuration, natsContext, diagnostics });
  }
}

// Encapsulated error handling for createStream()
async function reportCreateStreamError({ err, name, configuration, natsContext, diagnostics }) {
  const msg = String(err?.message || "").toLowerCase();
  const isOverlap = msg.includes("subjects overlap with an existing stream");

  if (isOverlap) {
    const meta = await getOverlappingStreamInfo({
      natsContext,
      attemptedName: name,
      subjects: configuration?.subjects,
    });
    diagnostics.error(
      Codes.STREAM_SUBJECT_OVERLAP,
      'Subjects overlap with an existing stream',
      { name, configuration, ...meta },
      { cause: err },
    );
  } else {
    diagnostics.error(
      Codes.STREAM_CREATE_FAILED,
      `Failed to create stream ${name}`,
      { name, configuration },
      { cause: err },
    );
  }
}


// When a stream creation fails with a JetStream subject overlap, this helper
// finds the conflicting stream(s) for the provided subjects and logs details.
async function getOverlappingStreamInfo({ natsContext, attemptedName, subjects }) {
  try {
    const jsm = await natsContext.jetstreamManager();
    const conflicting = new Map(); // name -> info

    for (const s of subjects || []) {
      try {
        const name = await jsm.streams.find(s);
        if (!conflicting.has(name)) {
          try {
            const si = await jsm.streams.info(name);
            conflicting.set(name, {
              name,
              subjects: si?.config?.subjects,
              retention: si?.config?.retention,
              storage: si?.config?.storage,
              replicas: si?.config?.num_replicas,
              created: si?.created || undefined,
              state: {
                messages: si?.state?.messages,
                bytes: si?.state?.bytes,
                consumer_count: si?.state?.consumer_count,
              },
            });
          } catch {
            // if info fails, at least capture the name
            conflicting.set(name, { name });
          }
        }
      } catch {
        // find() throws if no stream matches subject — ignore
      }
    }

    if (conflicting.size > 0) {
      return {
        attempted_name: attemptedName,
        attempted_subjects: subjects,
        conflicting_streams: Array.from(conflicting.values()),
      };
    }
  } catch {
    // best-effort — do not throw from diagnostic reporter
  }
  return { attempted_name: attemptedName, attempted_subjects: subjects };
}
