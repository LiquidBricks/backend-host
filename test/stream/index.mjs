import test, { before, after } from 'node:test'
import assert from 'node:assert/strict'

import { createStream } from '../../stream/index.js'
import { diagnostics as makeDiagnostics } from '@liquid-bricks/lib-diagnostics'
import { Codes } from '../../codes.js'
import createNatsContext from '@liquid-bricks/lib-nats-context';
import { serviceConfiguration } from '../../provider/serviceConfiguration/dotenv/index.js'

const { NATS_IP_ADDRESS } = serviceConfiguration()

let natsContext
let diagnostics

before(async () => {
  natsContext = createNatsContext({ servers: NATS_IP_ADDRESS })
  diagnostics = makeDiagnostics()
  await natsContext.connection()
})

after(async () => {
  try { const nc = await natsContext?.connection(); await nc?.close() } catch { }
})

test('stream/index#createStream: creates stream with provided subjects', async () => {
  const name = `bb_gen_${Date.now()}`
  const configuration = { subjects: ['unit.foo.*', 'unit.bar'] }
  const jsm = await natsContext.jetstreamManager()
  try {
    await createStream({ name, natsContext, diagnostics, configuration })
    const info = await jsm.streams.info(name)
    const subjects = info?.config?.subjects || []
    for (const s of configuration.subjects) assert.ok(subjects.includes(s), `missing subject ${s}`)
    assert.equal(subjects.length, configuration.subjects.length)
  } finally {
    try { await jsm.streams.delete(name) } catch { }
  }
})

test('stream/index#createStream: overlap throws DiagnosticError and preserves existing stream', async () => {
  const nameA = `bb_gen_A_${Date.now()}`
  const nameB = `bb_gen_B_${Date.now()}`
  const configuration = { subjects: ['unit.overlap.*'] }
  const jsm = await natsContext.jetstreamManager()
  try {
    await createStream({ name: nameA, natsContext, diagnostics, configuration })

    let err
    try {
      await createStream({ name: nameB, natsContext, diagnostics, configuration })
    } catch (e) { err = e }

    assert.ok(err instanceof diagnostics.DiagnosticError, 'expected DiagnosticError')
    assert.equal(err.code, Codes.STREAM_SUBJECT_OVERLAP)

    const infoA = await jsm.streams.info(nameA)
    assert.equal(infoA?.config?.name, nameA)

    let bExists = true
    try { await jsm.streams.info(nameB) } catch { bExists = false }
    assert.equal(bExists, false)
  } finally {
    try { await jsm.streams.delete(nameA) } catch { }
    try { await jsm.streams.delete(nameB) } catch { }
  }
})
