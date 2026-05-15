import { Umzug } from 'umzug'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { createNatsContext } from '../util/natsContext.js'
import { NatsKvStorage } from '../provider/migrations/natsKvStorage.mjs'
import { serviceConfiguration } from '../provider/serviceConfiguration/dotenv/index.js'

const BUCKET_NAME = 'migrations'

export const MIGRATIONS_VERSIONS_GLOB = 'migrations/versions/*.mjs'
const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
export const MIGRATION_TEMPLATE_PATH = path.resolve(__dirname, 'template/migration.mjs')

async function main() {
  const { NATS_IP_ADDRESS } = serviceConfiguration()
  const natsContext = createNatsContext({ servers: NATS_IP_ADDRESS })
  await natsContext.connection()
  const kvBucket = await natsContext.bucket(BUCKET_NAME)

  const storage = new NatsKvStorage({ bucket: kvBucket, key: 'executed' })

  const parent = new Umzug({
    migrations: { glob: MIGRATIONS_VERSIONS_GLOB },
    context: {},
    logger: console,
  })

  const umzug = new Umzug({
    context: {},
    storage,
    logger: console,
    migrations: async (ctx) => (await parent.migrations(ctx))
      .sort((a, b) => b.path.localeCompare(a.path)),
  })

  const previouslyExecuted = await storage.executed()
  if (!previouslyExecuted || previouslyExecuted.length === 0) {
    console.info('[migrate] No migrations have been run yet.')
  } else {
    const last = previouslyExecuted[previouslyExecuted.length - 1]
    console.info(`[migrate] Last executed migration: ${last}`)
  }
  const result = await umzug.up()
  const names = result.map(r => r.name)
  if (names.length === 0) {
    console.info('[migrate] No new migrations to run.')
  }
  console.log('[migrate] Executed migrations:', names)
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((err) => {
    console.error('[migrate] Error running migrations:', err)
    process.exit(1)
  })
}
