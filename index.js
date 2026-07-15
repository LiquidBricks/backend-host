import { createHandler } from 'graphql-http/lib/use/express';
import express from 'express';
import cors from 'cors';
import cron from 'node-cron'
import { createServer } from 'node:http'
import { schema } from '@liquid-bricks/iface-graphql/schema';
import { eventstream } from '@liquid-bricks/iface-eventstream';
import { Consumer as orchestrator } from '@liquid-bricks/svc-component-orchestrator/consumer';
import { Consumer as domainProjector } from '@liquid-bricks/svc-domain-projector/consumer';
import { collector } from '@liquid-bricks/obs-collector/collector';
import { gateway } from '@liquid-bricks/gw-ws-components/gateway';
import { Graph } from '@liquid-bricks/lib-nats-graph/graph';
import createNatsContext from '@liquid-bricks/lib-nats-context';
import { diagnostics as createDiagnostics } from '@liquid-bricks/lib-diagnostics'
import { createNatsMetrics } from '@liquid-bricks/lib-diagnostics/metrics/nats'
import { createNatsLogger } from '@liquid-bricks/lib-diagnostics/loggers/nats'
import { create as createTelemetrySubject } from '@liquid-bricks/lib-nats-subject/create/telemetry'
import { create as createBasicSubject } from '@liquid-bricks/lib-nats-subject/create/basic'
import { diagnostics as diagnosticsSubjectFactory } from '@liquid-bricks/lib-nats-subject'
import { events as natsEvents } from '@liquid-bricks/lib-nats-subject/events/nats'
import { serviceConfiguration } from './provider/serviceConfiguration/dotenv/index.js'
import { RetentionPolicy } from '@nats-io/jetstream'
import { createStream as createGenericStream } from './stream/index.js'
import { resetNatsFactoryDefaults } from './stream/helper.js';

const formattedTimestamp = () => {
  const now = new Date()
  const month = String(now.getMonth() + 1).padStart(2, '0')
  const day = String(now.getDate()).padStart(2, '0')
  const year = now.getFullYear()
  const hours = String(now.getHours()).padStart(2, '0')
  const minutes = String(now.getMinutes()).padStart(2, '0')
  return `${month}-${day}-${year} ${hours}:${minutes}`
}

process.on('SIGTERM', () => {
  console.log('Received SIGTERM signal. Shutting down gracefully...');
  // Perform cleanup actions like closing database connections, etc.
  // ...
  process.exit(0); // Exit with success code after cleanup
});


cron.schedule('*/1 * * * *', () => {
  console.log(`Current time ${formattedTimestamp()}`)
})
console.log(`Current time ${formattedTimestamp()}`)


const { NATS_IP_ADDRESS, PORT } = serviceConfiguration()
const parsedPort = Number(PORT)
const port = Number.isFinite(parsedPort) && PORT !== '' ? parsedPort : 4000
const natsContext = createNatsContext({ servers: NATS_IP_ADDRESS })
const diagnostics = createDiagnostics({
  context: () => ({ env: 'prod', service: 'backend-host' }),
  logger: createNatsLogger({
    natsContext,
    subject: () => 'tele.log.v1',
  }),
  metrics: createNatsMetrics({
    natsContext,
    subject: (kind) => {
      const natsMetricSubjectMapping = {
        timing: 'histogram',
        count: 'counter',
      }
      return createTelemetrySubject()
        .metric()
        .entity(natsMetricSubjectMapping[kind])
        .version("v1")
        .build()
    }
  }),
})
const graph = Graph({
  kv: 'nats',
  kvConfig: { servers: NATS_IP_ADDRESS, bucket: 'graph' },
  diagnostics,
})

const COMPONENT_SERVICE_STREAM_NAME = 'COMPONENT_SERVICE_STREAM'
const DIAGNOSTICS_STREAM_NAME = 'DIAGNOSTICS_STREAM'
const gatewayComputeFunctionSubject = createBasicSubject(natsEvents['*'].gateway['*']['*'].cmd.component.compute_function.v1['*'])
  .forSubscribe()
  .env('prod')
  .id('>')
  .build()
const domainVertexGateResultComputedSubject = createBasicSubject(
  natsEvents['*'].domain['*']['*'].vertex.gateInstanceRef.result_computed.v1['*'],
).forSubscribe().env('prod').build()
const domainVertexStateMachineCompletedSubject = createBasicSubject(
  natsEvents['*'].domain['*']['*'].vertex.stateMachine.completed.v1['*'],
).forSubscribe().env('prod').build()
const domainVertexStateMachineStartedSubject = createBasicSubject(
  natsEvents['*'].domain['*']['*'].vertex.stateMachine.started.v1['*'],
).forSubscribe().env('prod').build()
const DIAGNOSTICS_SUBJECTS = [
  diagnosticsSubjectFactory.create(natsEvents.tele['>']).forSubscribe().build(),
  diagnosticsSubjectFactory.create(natsEvents.metrics['>']).forSubscribe().build(),
]
const COMPONENT_SERVICE_SUBJECTS = [
  createBasicSubject(natsEvents['*'].component_service['*']['*'].cmd['>']).forSubscribe().env('prod').build(),
  createBasicSubject(natsEvents['*'].component_service['*']['*'].evt['>']).forSubscribe().env('prod').build(),
  createBasicSubject(natsEvents['*'].component_service['*']['*'].exec['>']).forSubscribe().env('prod').build(),
  createBasicSubject(natsEvents['*'].domain['*']['*'].edge['>']).forSubscribe().env('prod').build(),
  domainVertexGateResultComputedSubject,
  domainVertexStateMachineCompletedSubject,
  domainVertexStateMachineStartedSubject,
  gatewayComputeFunctionSubject,
]
const UNLIMITED_LIMITS = {
  max_msgs: -1,
  max_msgs_per_subject: -1,
  max_bytes: -1,
  max_age: 0,
  max_msg_size: -1,
}

Promise.resolve()
  // Recreate streams defined in migrations to ensure desired config
  .then(async () => {
    // await resetNatsFactoryDefaults({ natsContext })
    const jsm = await natsContext.jetstreamManager();

    try { await jsm.streams.delete(COMPONENT_SERVICE_STREAM_NAME); } catch (_) { /* ignore if not found */ }
    try { await jsm.streams.delete(DIAGNOSTICS_STREAM_NAME); } catch (_) { /* ignore if not found */ }

    await createGenericStream({
      name: DIAGNOSTICS_STREAM_NAME,
      natsContext,
      diagnostics,
      configuration: {
        retention: RetentionPolicy.Limits,
        subjects: DIAGNOSTICS_SUBJECTS,
        ...UNLIMITED_LIMITS,
      },
    });

    await createGenericStream({
      name: COMPONENT_SERVICE_STREAM_NAME,
      natsContext,
      diagnostics,
      configuration: {
        retention: RetentionPolicy.Limits,
        subjects: COMPONENT_SERVICE_SUBJECTS,
        ...UNLIMITED_LIMITS,
      },
    });
  })
  .then(() => orchestrator({
    streamName: COMPONENT_SERVICE_STREAM_NAME,
    natsContext,
    g: graph.g,
    diagnostics,
  }))
  .then(() => domainProjector({
    streamName: COMPONENT_SERVICE_STREAM_NAME,
    natsContext,
    g: graph.g,
    diagnostics,
  }))
  .then(() => collector({
    streamName: DIAGNOSTICS_STREAM_NAME,
    natsContext,
    diagnostics,
  }))


  .then(async () => {
    const app = express();
    const corsConfig = {
      origin: true,
      credentials: true,
    };

    const corsMiddleware = cors(corsConfig);

    app.use(corsMiddleware);
    app.options('/graphql', corsMiddleware);
    app.options('/eventstream', corsMiddleware);
    app.get('/eventstream', corsMiddleware, eventstream({
      natsContext,
      diagnostics,
      streamName: COMPONENT_SERVICE_STREAM_NAME,
      subjects: COMPONENT_SERVICE_SUBJECTS,
    }));

    app.all(
      '/graphql',
      corsMiddleware,
      createHandler({
        schema,
        context: async (req) => {
          const rawCorrelation = req?.headers?.['x-correlation-id']
          const toCorrelationId = (value) => {
            if (typeof value === 'string') {
              const trimmed = value.trim()
              return trimmed.length ? trimmed : undefined
            }
            if (Array.isArray(value)) {
              for (const candidate of value) {
                const match = toCorrelationId(candidate)
                if (match) return match
              }
            }
            return undefined
          }

          return {
            natsContext,
            g: graph.g,
            diagnostics: diagnostics.child({ system: 'graphql', correlationId: toCorrelationId(rawCorrelation) }),
          }
        },
      }),
    );

    // app.get('/ruru', (_req, res) => {
    //   res.type('html');
    //   res.end(ruruHTML({ endpoint: '/graphql' }));
    // });

    const server = createServer(app);

    await gateway({
      server,
      path: '/componentAgent',
      streamName: COMPONENT_SERVICE_STREAM_NAME,
      natsContext,
      diagnostics,
    });

    server.listen(port);
    return server;
  })
  .then(() => console.log(`Running a GraphQL API server at http://localhost:${port}/graphql`))
