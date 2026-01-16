import { Connection, connect } from 'amqplib';
import { assert, expect } from 'chai';
import { Termination, terminate } from '../common/termination';
import { Container, start } from '../docker/container';
import { resources } from '../index';
import { Resource, init } from '../resources';
import { AmqpInput, AmqpOutput, IOType, testFlow } from './flow-test';

describe('Flow test', () => {
    const inputPayload = {
        "data": {
            "propA": -0.05,
            "obj": {
                "propB": "test",
            }
        },
        "test": [
            false,
            true
        ],
    };
    const expectedOutput = inputPayload;

    const terminables: Termination[] = [];
    let conn: Connection;

    before(async () => {
        const container: Container =                 {
            image: 'rabbitmq:3.8.2-management',
            name: 'test-rabbitmq',
            ports: [
                {
                    host: 5672,
                    container: 5672,
                },
                {
                    host: 15672,
                    container: 15672,
                },
            ],
            readinessProbe: {
                failureThreshold: 5,
                periodSeconds: 30,
                fn: async () => {
                    const probeConn = await connect("amqp://localhost:5672");
                    // Must close connection or an error will be thrown when the
                    // rabbitmq container exits
                    await probeConn.close();
                },
            },
            stdout: process.stdout,
            stderr: process.stderr,
        };

        try {
            terminables.push(await start(container));

            const amqpRsc = resources.amqp("amqp://localhost:5672");
            const valuePairs = await init({
                rabbitmq: amqpRsc,
            });
            const [connection, terminateAmqp] = valuePairs.rabbitmq as Resource<Connection>;
            conn = connection;
            // Must put connection terminable at start of array to ensure it is terminated
            // before rabbitmq container
            terminables.unshift(terminateAmqp);

            const setupChan = await conn.createChannel();
            await setupChan.assertExchange("test.ex", "direct");
            await setupChan.assertQueue("test.queue.1");
            await setupChan.assertQueue("test.queue.2");
            await setupChan.bindQueue("test.queue.1", "test.ex", "queue.1.key");
            await setupChan.bindQueue("test.queue.2", "test.ex", "queue.2.key");
            await setupChan.close();
        } catch {
            await terminate(...terminables);
        }
    });

    after(async () => {
        await terminate(...terminables);
    });

    it('should start', async () => {
        await testFlow(conn, {
            type: IOType.AMQP,
            exchange: "test.ex",
            routingKey: "queue.1.key",
            payload: inputPayload
        } as AmqpInput, {
            expectedOutput,
            type: IOType.AMQP,
            queues: ["test.queue.1", "test.queue.2"],
            expectedQueue: "test.queue.1",
        } as AmqpOutput);
    });

    it('should fail when message is not routed', async () => {
        try {
            await testFlow(conn, {
                type: IOType.AMQP,
                exchange: "test.ex",
                routingKey: "queue.OTHER.key",
                payload: inputPayload
            } as AmqpInput, {
                expectedOutput,
                type: IOType.AMQP,
                queues: ["test.queue.1", "test.queue.2"],
                expectedQueue: "test.queue.1",
            } as AmqpOutput);
        } catch (e) {
            expect(e).to.exist;

            return;
        }

        assert.fail("Did not throw error");
    });

    it('should fail when message is received and no message is expected', async () => {
        try {
            await testFlow(conn, {
                type: IOType.AMQP,
                exchange: "test.ex",
                routingKey: "queue.1.key",
                payload: inputPayload
            } as AmqpInput, {
                expectedOutput,
                type: IOType.AMQP,
                queues: ["test.queue.1", "test.queue.2"],
                expectedQueue: "",
            } as AmqpOutput);
        } catch (e) {
            expect(e).to.exist;

            return;
        }

        assert.fail("Did not throw error");
    });

    it('should fail when payload does not match', async () => {
        try {
            await testFlow(conn, {
                type: IOType.AMQP,
                exchange: "test.ex",
                routingKey: "queue.1.key",
                payload: { test: "UNEXPECTED" }
            } as AmqpInput, {
                expectedOutput,
                type: IOType.AMQP,
                queues: ["test.queue.1", "test.queue.2"],
                expectedQueue: "test.queue.1",
            } as AmqpOutput);
        } catch (e) {
            expect(e).to.exist;

            return;
        }

        assert.fail("Did not throw error");
    });
});