import { ObjectId } from 'mongodb';
import config from '../../src/config';
import { buildApp, App } from '../../src/app';
import Agenda from 'agenda';
import nock from 'nock';

describe('jobs-executions', () => {
    let app: App;
    let server;

    beforeEach(async () => {
        const options = {
            databaseName: `test-${new ObjectId()}`,
            databaseUrl: config.mongodb.databaseUrl
        };
        app = await buildApp(options);
        server = app.getServer();
    });

    afterEach(async () => {
        await app.getDatabase().dropDatabase();
        await app.close();
    });

    async function executeJob(jobId: ObjectId, success: boolean, jobExecutionHandler) {
        const job = {
            agenda: null,
            attrs: {
                name: 'a job',
                repeatInterval: null,
                type: 'normal',
                _id: jobId,
                data: {
                    url: 'https://example.org',
                    method: 'GET'
                }
            }
        } as unknown as Agenda.Job;

        const scope = nock('https://example.org').get('/').reply(success ? 200 : 502);

        let doneResolve;
        const donePromise = new Promise(resolve => {
            doneResolve = resolve;
        });

        jobExecutionHandler(job, doneResolve);

        await donePromise;

        expect(scope.isDone()).toBe(true);
    }

    describe('get', () => {
        it('should return 200 with array of jobs executions', async () => {
            const jobId = new ObjectId();

            await executeJob(jobId, true, app.getJobExecutionHandler());

            const response = await server.inject({
                method: 'GET',
                url: '/jobs-executions'
            });
            expect(response.statusCode).toBe(200);
            expect(response.headers['content-type']).toBe('application/json; charset=utf-8');
            const listResponse = JSON.parse(response.payload);
            expect(listResponse.results.length).toBe(1);
            const jobExecution = listResponse.results[0];
            expect(ObjectId.isValid(jobExecution.id)).toBe(true);
            expect(jobExecution.jobId).toBe(jobId.toHexString());
            expect(jobExecution.success).toBe(true);
            expect(jobExecution.failureReason).toBe(undefined);
        });

        it('should return 200 with array of jobs executions filtered by JobId', async () => {
            const jobId1 = new ObjectId();
            const jobId2 = new ObjectId();

            await executeJob(jobId1, true, app.getJobExecutionHandler());
            await executeJob(jobId2, false, app.getJobExecutionHandler());

            const response1 = await server.inject({
                method: 'GET',
                url: '/jobs-executions?jobId=' + jobId1.toHexString()
            });
            expect(response1.statusCode).toBe(200);
            expect(response1.headers['content-type']).toBe('application/json; charset=utf-8');
            const listResponse1 = JSON.parse(response1.payload);
            expect(listResponse1.results.length).toBe(1);
            const jobExecution1 = listResponse1.results[0];
            expect(ObjectId.isValid(jobExecution1.id)).toBe(true);
            expect(jobExecution1.jobId).toBe(jobId1.toHexString());
            expect(jobExecution1.success).toBe(true);
            expect(jobExecution1.failureReason).toBe(undefined);

            const response2 = await server.inject({
                method: 'GET',
                url: '/jobs-executions?jobId=' + jobId2.toHexString()
            });
            expect(response2.statusCode).toBe(200);
            expect(response2.headers['content-type']).toBe('application/json; charset=utf-8');
            const listResponse2 = JSON.parse(response2.payload);
            expect(listResponse2.results.length).toBe(1);
            const jobExecution2 = listResponse2.results[0];
            expect(ObjectId.isValid(jobExecution2.id)).toBe(true);
            expect(jobExecution2.jobId).toBe(jobId2.toHexString());
            expect(jobExecution2.success).toBe(false);
            expect(jobExecution2.failureReason).toBe('502 - Bad Gateway');
        });

        it('should set next and not prev link in first page when jobs returned match page size', async () => {
            await Promise.all([1, 2, 3, 4, 5].map(value => {
                const jobId = new ObjectId();
                return executeJob(jobId, true, app.getJobExecutionHandler());
            }));
            const response = await server.inject({
                method: 'GET',
                url: '/jobs-executions?page=1&pageSize=2'
            });
            const payloadResponse = JSON.parse(response.payload);
            expect(payloadResponse.prev).toBeUndefined();
            expect(payloadResponse.next).toBe('http://localhost:8888/jobs-executions?page=2&pageSize=2');
        });

        it('should not set next and not prev link in first page when jobs returned are lower than page size', async () => {
            await Promise.all([1, 2].map(value => value => {
                const jobId = new ObjectId();
                return executeJob(jobId, true, app.getJobExecutionHandler());
            }));
            const response = await server.inject({
                method: 'GET',
                url: '/jobs-executions?page=1&pageSize=3'
            });
            const payloadResponse = JSON.parse(response.payload);
            expect(payloadResponse.prev).toBeUndefined();
            expect(payloadResponse.next).toBeUndefined();
        });

        it('should set next and prev link if a middle page', async () => {
            await Promise.all([1, 2, 3, 4, 5].map(value => {
                const jobId = new ObjectId();
                return executeJob(jobId, true, app.getJobExecutionHandler());
            }));
            const response = await server.inject({
                method: 'GET',
                url: '/jobs-executions?page=2&pageSize=2'
            });
            const payloadResponse = JSON.parse(response.payload);
            expect(payloadResponse.prev).toBe('http://localhost:8888/jobs-executions?page=1&pageSize=2');
            expect(payloadResponse.next).toBe('http://localhost:8888/jobs-executions?page=3&pageSize=2');
        });

        it('should return 400 with invalid page query string', async () => {
            const response = await server.inject({
                method: 'GET',
                url: '/jobs-executions?page=invalid'
            });
            expect(response.statusCode).toBe(400);
            expect(response.headers['content-type']).toBe('application/json; charset=utf-8');
            expect(response.payload).toBe(JSON.stringify({
                statusCode: 400,
                error: 'Bad Request',
                message: 'querystring.page should be integer'
            }));
        });

        it('should return 400 with invalid pageSize query string', async () => {
            const response = await server.inject({
                method: 'GET',
                url: '/jobs-executions?pageSize=invalid'
            });
            expect(response.statusCode).toBe(400);
            expect(response.headers['content-type']).toBe('application/json; charset=utf-8');
            expect(response.payload).toBe(JSON.stringify({
                statusCode: 400,
                error: 'Bad Request',
                message: 'querystring.pageSize should be integer'
            }));
        });

        it('should return 400 with pageSize query string greater than 100', async () => {
            const response = await server.inject({
                method: 'GET',
                url: '/jobs-executions?pageSize=101'
            });
            expect(response.statusCode).toBe(400);
            expect(response.headers['content-type']).toBe('application/json; charset=utf-8');
            expect(response.payload).toBe(JSON.stringify({
                statusCode: 400,
                error: 'Bad Request',
                message: 'querystring.pageSize should be <= 100'
            }));
        });

        it('should return 400 with pageSize query string lesser than 1', async () => {
            const response = await server.inject({
                method: 'GET',
                url: '/jobs-executions?pageSize=0'
            });
            expect(response.statusCode).toBe(400);
            expect(response.headers['content-type']).toBe('application/json; charset=utf-8');
            expect(response.payload).toBe(JSON.stringify({
                statusCode: 400,
                error: 'Bad Request',
                message: 'querystring.pageSize should be >= 1'
            }));
        });

        it('should return 400 with page query string lesser than 1', async () => {
            const response = await server.inject({
                method: 'GET',
                url: '/jobs-executions?page=0'
            });
            expect(response.statusCode).toBe(400);
            expect(response.headers['content-type']).toBe('application/json; charset=utf-8');
            expect(response.payload).toBe(JSON.stringify({
                statusCode: 400,
                error: 'Bad Request',
                message: 'querystring.page should be >= 1'
            }));
        });

        it('should return 400 with invalid jobId query string', async () => {
            const response = await server.inject({
                method: 'GET',
                url: '/jobs-executions?jobId=invalid_value'
            });
            expect(response.statusCode).toBe(400);
            expect(response.headers['content-type']).toBe('application/json; charset=utf-8');
            expect(response.payload).toBe(JSON.stringify({
                statusCode: 400,
                error: 'Bad Request',
                message: 'jobId format is invalid'
            }));
        });
    });
});