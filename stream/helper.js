// Utilities for managing NATS/JetStream state

// Resets NATS JetStream-related state to a factory-like default by removing:
// - All Streams (and thus their Consumers)
// - All Key-Value buckets (if supported by the client)
// - All Object Store buckets (if supported by the client)
// This is intentionally tolerant: unknown/unsupported operations are ignored.
export async function resetNatsFactoryDefaults({ natsContext }) {
  const jsm = await natsContext.jetstreamManager();

  // Delete all streams
  try {
    const names = new Set();
    try {
      const lister = await jsm.streams.list();
      for await (const page of lister) {
        const n = page?.config?.name ?? page?.name ?? page;
        if (typeof n === 'string') names.add(n);

      }
    } catch (_) {
      // fallback to names() if list() is not available
      try {
        const lister = await jsm.streams.names();
        for await (const page of lister) {
          console.log({ b: 2, n })
          if (typeof n === 'string') names.add(n);
        }
      } catch { /* ignore */ }
    }
    for (const name of names) {
      try {
        console.log('deleteing', name)
        await jsm.streams.delete(name);
      } catch { console.error('unable to delete', name) }
    }
  } catch { /* ignore */ }

  // Delete all Key-Value buckets, if supported
  try {
    const lister = await jsm.kv.list();
    for await (const page of lister) {
      for (const b of page) {
        const bucket = b?.bucket ?? b;
        if (typeof bucket === 'string') {
          try { await jsm.kv.delete(bucket); } catch { /* ignore */ }
        }
      }
    }
  } catch { /* ignore: kv not supported */ }

  // Delete all Object Store buckets, if supported
  try {
    const lister = await jsm.os.list();
    for await (const page of lister) {
      for (const b of page) {
        const bucket = b?.bucket ?? b;
        if (typeof bucket === 'string') {
          try { await jsm.os.delete(bucket); } catch { /* ignore */ }
        }
      }
    }
  } catch { /* ignore: object store not supported */ }
}

// Prints structured information about the connected NATS/JetStream environment.
// Attempts to collect:
// - Server info (version, cluster, features, RTT, stats)
// - JetStream account info (limits/usage)
// - Streams (+ basic consumer summaries)
// - Key-Value buckets (via JSM if available, otherwise via Kvm)
// - Object Store buckets (if supported)
export async function info({ natsContext }) {
  try {
    const nc = await natsContext.connection();
    const jsm = await natsContext.jetstreamManager().catch(() => undefined);

    const toArray = async (lister, mapper = (v) => v) => {
      const out = [];
      if (!lister) return out;
      for await (const page of lister) {
        out.push(mapper(page));
      }
      return out;
    };

    // Connection/server basics
    let rttMs = undefined;
    try { rttMs = await nc.rtt(); } catch { }
    const stats = nc.stats?.() || {};
    const serverInfo = nc.info || {};
    const featuresRaw = nc.features;
    const featureNames = [
      'js_kv',
      'js_objectstore',
      'js_pull_max_bytes',
      'js_new_consumer_create',
      'js_allow_direct',
      'js_multiple_consumer_filter',
      'js_simplification',
      'js_stream_consumer_metadata',
      'js_consumer_filter_subjects',
      'js_stream_first_seq',
      'js_stream_subject_transform',
      'js_stream_source_subject_transform',
      'js_stream_compression',
      'js_default_consumer_limits',
      'js_batch_direct_get',
      'js_priority_groups',
    ];
    const features = {};
    try {
      for (const f of featureNames) {
        try {
          const s = featuresRaw.get(f);
          features[f] = { supported: !!s?.ok, min: s?.min ?? 'unknown' };
        } catch {
          features[f] = { supported: false, min: 'unknown' };
        }
      }
    } catch { }

    // JetStream account info
    let accountInfo = undefined;
    try {
      accountInfo = await jsm?.getAccountInfo();
    } catch { }

    // Streams and consumers summary
    const streams = [];
    try {
      const lister = await jsm?.streams.list();
      const all = await toArray(lister);
      console.log({ all })
      for (const si of all) {
        const name = si?.config?.name ?? si?.name;
        const consumers = [];
        try {
          const cl = await jsm?.consumers.list(name);
          const ci = await toArray(cl);
          for (const c of ci) {
            const cfg = c?.config ?? {};
            consumers.push({
              name: cfg?.name || cfg?.durable_name,
              durable_name: cfg?.durable_name,
              ack_policy: cfg?.ack_policy,
              deliver_policy: cfg?.deliver_policy,
              filter_subject: cfg?.filter_subject,
              filter_subjects: cfg?.filter_subjects,
              max_ack_pending: cfg?.max_ack_pending,
            });
          }
        } catch (err) { console.log({ err }) }
        streams.push({
          name,
          subjects: si?.config?.subjects,
          retention: si?.config?.retention,
          storage: si?.config?.storage,
          replicas: si?.config?.num_replicas,
          max_msgs: si?.config?.max_msgs,
          max_msgs_per_subject: si?.config?.max_msgs_per_subject,
          max_bytes: si?.config?.max_bytes,
          sealed: !!si?.config?.sealed,
          created: si?.created || undefined,
          state: {
            messages: si?.state?.messages,
            bytes: si?.state?.bytes,
            first_seq: si?.state?.first_seq,
            last_seq: si?.state?.last_seq,
            consumer_count: si?.state?.consumer_count,
          },
          consumers,
        });
      }
    } catch (err) { console.log({ err }) }

    // KV buckets (prefer JSM if available, else Kvm)
    const kvBuckets = [];
    try {
      let kvList = [];
      try {
        const l = await jsm?.kv?.list();
        kvList = await toArray(l);
        for (const b of kvList) {
          const bucket = b?.bucket ?? b?.bucket?.bucket ?? b;
          kvBuckets.push({
            bucket,
            values: b?.values,
            history: b?.history,
            replicas: b?.replicas,
            storage: b?.storage ?? b?.backingStore,
            ttl_ms: b?.ttl || b?.markerTTL,
            size_bytes: b?.size,
            description: b?.description,
          });
        }
      } catch {
        // fallback: use Kvm helper
        try {
          const Kvm = await natsContext.Kvm?.();
          // when called as function, Kvm resolves to a class instance
          const l = await Kvm?.list();
          for await (const page of l || []) {
            for (const s of page) {
              kvBuckets.push({
                bucket: s?.bucket,
                values: s?.values,
                history: s?.history,
                replicas: s?.replicas,
                storage: s?.storage ?? s?.backingStore,
                ttl_ms: s?.ttl || s?.markerTTL,
                size_bytes: s?.size,
                description: s?.description,
              });
            }
          }
        } catch { }
      }
    } catch { }

    // Object Stores (if supported)
    const objectStores = [];
    try {
      const l = await jsm?.os?.list();
      const arr = await toArray(l);
      for (const b of arr) {
        const bucket = b?.bucket ?? b;
        objectStores.push({ bucket });
      }
    } catch { }

    // System/user context, if accessible
    let sysUser = undefined;
    try { sysUser = await nc.context(); } catch { }

    const report = {
      connection: {
        server: nc.getServer?.() || undefined,
        rtt_ms: rttMs,
        stats,
        info: serverInfo,
        features,
      },
      jetstream: {
        account: accountInfo,
        streams,
        kv_buckets: kvBuckets,
        object_stores: objectStores,
      },
      system_user: sysUser,
      timestamp: new Date().toISOString(),
    };

    console.info('[NATS] Environment info', report);
    return report;
  } catch (err) {
    console.info('[NATS] Unable to collect info', { error: String(err) });
    return undefined;
  }
}
