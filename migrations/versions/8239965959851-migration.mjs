// Provide ready-to-use natsContext and diagnostics for migrations
import { createNatsContext } from '../../util/natsContext.js'
import { diagnostics as createDiagnostics } from '../../provider/diagnostics/index.js'
import { serviceConfiguration } from '../../provider/serviceConfiguration/dotenv/index.js'

import { RetentionPolicy } from '@nats-io/jetstream'
import { createStream as createGenericStream } from '../../stream/index.js'
import { resetNatsFactoryDefaults } from '../../stream/helper.js'
import { create as createBasicSubject } from '@liquid-bricks/lib-nats-subject/create/basic'
import { events as natsEvents } from '@liquid-bricks/lib-nats-subject/events/nats'

const { NATS_IP_ADDRESS } = serviceConfiguration()
export const natsContext = createNatsContext({ servers: NATS_IP_ADDRESS })
export const diagnostics = createDiagnostics()
const domainVertexGateResultComputedSubject = createBasicSubject({
  env: 'prod',
  ns: 'domain',
  tenant: '*',
  context: '*',
  channel: 'vertex',
  entity: 'gateInstanceRef',
  action: 'result_computed',
  version: 'v1',
  id: '*',
}).forSubscribe().build()
const domainVertexStateMachineCompletedSubject = createBasicSubject({
  env: 'prod',
  ns: 'domain',
  tenant: '*',
  context: '*',
  channel: 'vertex',
  entity: 'stateMachine',
  action: 'completed',
  version: 'v1',
  id: '*',
}).forSubscribe().build()
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
        createBasicSubject(natsEvents['*'].component_service['*']['*'].cmd['>']).forSubscribe().env('prod').build(),
        createBasicSubject(natsEvents['*'].component_service['*']['*'].evt['>']).forSubscribe().env('prod').build(),
        createBasicSubject(natsEvents['*'].component_service['*']['*'].exec['>']).forSubscribe().env('prod').build(),
        createBasicSubject(natsEvents['*'].domain['*']['*'].edge['>']).forSubscribe().env('prod').build(),
        domainVertexGateResultComputedSubject,
        domainVertexStateMachineCompletedSubject,
      ],
      max_msgs: -1,
      max_msgs_per_subject: -1,
      max_bytes: -1,
      max_age: 0,
      max_msg_size: -1,
    },
  })
}
