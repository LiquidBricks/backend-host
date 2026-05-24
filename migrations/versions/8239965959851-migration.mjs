// Provide ready-to-use natsContext and diagnostics for migrations
import { createNatsContext } from '../../util/natsContext.js'
import { diagnostics as createDiagnostics } from '../../provider/diagnostics/index.js'
import { serviceConfiguration } from '../../provider/serviceConfiguration/dotenv/index.js'

import { RetentionPolicy } from '@nats-io/jetstream'
import { createStream as createGenericStream } from '../../stream/index.js'
import { resetNatsFactoryDefaults } from '../../stream/helper.js'

const { NATS_IP_ADDRESS } = serviceConfiguration()
export const natsContext = createNatsContext({ servers: NATS_IP_ADDRESS })
export const diagnostics = createDiagnostics()

export async function up() {
  // Ensure component service streams exist (recreate to match desired config)
  await createGenericStream({
    name: 'DIAGNOSTICS_STREAM',
    natsContext,
    diagnostics,
    configuration: {
      retention: RetentionPolicy.Limits,
      subjects: ['diagnostics.*'],
      max_msgs: -1,
      max_msgs_per_subject: -1,
      max_bytes: -1,
      max_age: 0,
      max_msg_size: -1,
    },
  })

  await createGenericStream({
    name: 'COMPONENT_SERVICE_STREAM',
    natsContext,
    diagnostics,
    configuration: {
      retention: RetentionPolicy.Limits,
      subjects: [
        'prod.component-service.*.*.cmd.>',
        'prod.component-service.*.*.evt.>',
        'prod.component-service.*.*.exec.>',
      ],
      max_msgs: -1,
      max_msgs_per_subject: -1,
      max_bytes: -1,
      max_age: 0,
      max_msg_size: -1,
    },
  })
}
