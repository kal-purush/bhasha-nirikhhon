import 'core/boot/preloader';
import Server from 'core/http/server';
import Application from 'core/boot/application';
import * as dotenv from 'dotenv-safe';
import { OK } from 'http-status';
import mongoose from 'mongoose';
import request from 'supertest';

describe('[GET] - /api', () => {
    let server;
    const endpointToCall = '/';

    beforeAll(() => {
        process.env.NODE_ENV = 'test';
        dotenv.config();

        server = Server.init(Application).start();
    });

    afterAll(async () => {
        await mongoose.disconnect();
        server.close();
    });

    test('This should load with no errors', async () => {
        expect.assertions(2);
        const response = await request(server).get(endpointToCall);
        expect(response.status).toBe(OK);
        expect(response.body).toMatchObject({
            timestamp: expect.any(String),
        });
    });

    test('This should allow cors', async () => {
        expect.assertions(1);
        const response = await request(server).options(endpointToCall);
        expect(response.header['access-control-allow-origin']).toBe('*');
    });
});