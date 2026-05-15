// Provide ready-to-use natsContext and diagnostics for migrations
import { createNatsContext } from '../../util/natsContext.js'
import { diagnostics as createDiagnostics } from '../../provider/diagnostics/index.js'
import { serviceConfiguration } from '../../provider/serviceConfiguration/dotenv/index.js'

const { NATS_IP_ADDRESS } = serviceConfiguration()
export const natsContext = createNatsContext({ servers: NATS_IP_ADDRESS })
export const diagnostics = createDiagnostics()

export async function up() {
  // Example:
  // const jsm = await natsContext.jetstreamManager()
  // diagnostics.info('Running migration XYZ')
}
