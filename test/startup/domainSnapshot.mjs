import test from 'node:test'
import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'

const backendHostSource = await readFile(new URL('../../index.js', import.meta.url), 'utf8')
const packageJson = JSON.parse(await readFile(new URL('../../package.json', import.meta.url), 'utf8'))

test('backend host installs and starts the domain snapshot consumer', () => {
  assert.match(
    packageJson.dependencies['@liquid-bricks/svc-domain-snapshot'],
    /^github:LiquidBricks\/svc-domain-snapshot#[a-f0-9]{40}$/,
  )

  assert.match(
    backendHostSource,
    /import\s+\{\s*Consumer\s+as\s+domainSnapshot\s*\}\s+from\s+'@liquid-bricks\/svc-domain-snapshot\/consumer'/,
  )

  const projectorStart = backendHostSource.indexOf('.then(() => domainProjector({')
  const snapshotStart = backendHostSource.indexOf('.then(() => domainSnapshot({')
  const collectorStart = backendHostSource.indexOf('.then(() => collector({')

  assert.ok(projectorStart >= 0, 'domain projector startup is registered')
  assert.ok(snapshotStart > projectorStart, 'domain snapshot starts after the domain projector')
  assert.ok(collectorStart > snapshotStart, 'domain snapshot starts before the diagnostics collector')

  const snapshotRegistration = backendHostSource.slice(snapshotStart, collectorStart)
  assert.match(snapshotRegistration, /streamName:\s*COMPONENT_SERVICE_STREAM_NAME/)
  assert.match(snapshotRegistration, /natsContext/)
  assert.match(snapshotRegistration, /g:\s*graph\.g/)
  assert.match(snapshotRegistration, /diagnostics/)
})

test('backend stream retains componentInstance created domain facts', () => {
  const subjectsStart = backendHostSource.indexOf('const COMPONENT_SERVICE_SUBJECTS = [')
  const subjectsEnd = backendHostSource.indexOf(']\nconst UNLIMITED_LIMITS', subjectsStart)
  const componentServiceSubjects = backendHostSource.slice(subjectsStart, subjectsEnd)

  assert.ok(subjectsStart >= 0, 'component service subjects are declared')
  assert.ok(subjectsEnd > subjectsStart, 'component service subjects can be inspected')
  assert.match(
    backendHostSource,
    /vertex\.componentInstance\.created\.v1\['\*'\]/,
  )
  assert.match(
    componentServiceSubjects,
    /domainVertexComponentInstanceCreatedSubject/,
  )
})
