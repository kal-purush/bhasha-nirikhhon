import { expect } from 'chai';
import { Container, findID, start } from '../docker/container';
import { connect } from './amqp';

describe('Container readiness probes', () => {
    describe('AMQP probe', () => {
        it('should resolve a promise if a probe succeeded', async () => {
            const c: Container = {
                name: `${Date.now()}${Math.random()}`,
                image: 'rabbitmq',
                ports: [
                    {
                        host: 5672,
                        container: 5672,
                    },
                ],
                readinessProbe: {
                    initialDelaySeconds: 3,
                    failureThreshold: 5,
                    fn: connect(),
                },
                stdout: process.stdout,
                stderr: process.stderr,
            };

            const termination = await start(c);

            expect(termination).to.be.a('function');

            const foundID = await findID(c.name as string);

            await termination();

            expect(foundID).to.be.a('string');
        });
    });
});