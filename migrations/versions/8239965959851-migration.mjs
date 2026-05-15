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
  // Ensure per-consumer streams exist (recreate to match desired config)
  await createGenericStream({
    name: 'DIAGNOSTICS_STREAM',
    natsContext,
    diagnostics,
    configuration: {
      retention: RetentionPolicy.Workqueue,
      subjects: ['diagnostics.*'],
    },
  })

  await createGenericStream({
    name: 'COMPONENT_MANAGER_STREAM',
    natsContext,
    diagnostics,
    configuration: {
      retention: RetentionPolicy.Workqueue,
      subjects: [
        'component.command',
        'component.event',
        'componentInstance.command',
        'componentInstance.event',
      ],
    },
  })

  await createGenericStream({
    name: 'COMPONENT_EXECUTION_STREAM',
    natsContext,
    diagnostics,
    configuration: {
      retention: RetentionPolicy.Workqueue,
      subjects: [
        'componentNode.>',
      ],
    },
  })
}
