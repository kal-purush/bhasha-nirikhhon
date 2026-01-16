import Server from 'core/http/server';
import Application from 'core/boot/application';
import * as dotenv from 'dotenv-safe';
import {
    ACCEPTED,
    CONFLICT,
    CREATED, NOT_FOUND, NO_CONTENT, OK,
} from 'http-status';
import mongoose from 'mongoose';
import request from 'supertest';
import { IProduct } from 'interfaces/product';

describe('[GET] - /products', () => {
    let server;
    const endpointToCall = '/products';

    beforeAll(() => {
        process.env.NODE_ENV = 'test';
        dotenv.config();

        Application.start(() => {});
        server = Server.init(Application).start();
    });

    afterAll(async () => {
        await mongoose.disconnect();
        server.close();
    });

    test('This should show a list of products', async () => {
        expect.assertions(2);
        const response = await request(server).get(endpointToCall);
        expect(response.status).toBe(OK);
        expect(response.body).toMatchObject({
            items: {} as IProduct[],
        });
    });

    test('This should insert, show and remove product by SKU', async () => {
        const sku = String((Math.random() + 1).toString(36).substring(7));

        expect.assertions(6);
        const createResponse = await request(server).post(endpointToCall).send({
            sku,
            name: sku,
            price: 100.00,
            size: 'large',
            stockLevel: 100,
        });

        const retrieveResponse = await request(server).get(`${endpointToCall}/${sku}`);
        const deleteResponse = await request(server).delete(`${endpointToCall}/${sku}`);

        expect(createResponse.body).toBeDefined();
        expect(createResponse.status).toBe(CREATED);
        expect(retrieveResponse.body).toBeDefined();
        expect(retrieveResponse.body).toMatchObject({} as IProduct);
        expect(retrieveResponse.status).toBe(OK);
        expect(deleteResponse.status).toBe(NO_CONTENT);
    });

    test('This should not insert second time cause already exists', async () => {
        const sku = String((Math.random() + 1).toString(36).substring(7));

        expect.assertions(6);
        const creteFirstResponse = await request(server).post(endpointToCall).send({
            sku,
            name: sku,
            price: 100.00,
            size: 'large',
            stockLevel: 100,
        });

        const createSecondResponse = await request(server).post(endpointToCall).send({
            sku,
            name: sku,
            price: 100.00,
            size: 'large',
            stockLevel: 100,
        });

        const deleteResponse = await request(server).delete(`${endpointToCall}/${sku}`);

        expect(creteFirstResponse.body).toBeDefined();
        expect(creteFirstResponse.status).toBe(CREATED);
        expect(createSecondResponse.status).toBe(CONFLICT);
        expect(createSecondResponse.body).toBeDefined();
        expect(createSecondResponse.body.message).toBe(`product with SKU:${sku} already exists.`);
        expect(deleteResponse.status).toBe(NO_CONTENT);
    });

    test('This should insert, update and delete product by SKU', async () => {
        const sku = String((Math.random() + 1).toString(36).substring(7));

        expect.assertions(5);
        const createResponse = await request(server).post(endpointToCall).send({
            sku,
            name: sku,
            price: 100.00,
            size: 'large',
            stockLevel: 100,
        });

        const updateResponse = await request(server).put(`${endpointToCall}/${sku}`).send({
            sku,
            name: sku,
            price: 120.00,
            size: 'large',
            stockLevel: 200,
        });

        const deleteResponse = await request(server).delete(`${endpointToCall}/${sku}`);

        expect(createResponse.body).toBeDefined();
        expect(createResponse.status).toBe(CREATED);
        expect(updateResponse.body).toBeDefined();
        expect(updateResponse.status).toBe(ACCEPTED);
        expect(deleteResponse.status).toBe(NO_CONTENT);
    });

    test('This should not update product by SKU cause not exists', async () => {
        expect.assertions(3);

        const updateResponse = await request(server).put(`${endpointToCall}/inexistent-sku`).send({
            sku: 'inexistent-sku',
            name: 'inexistent product',
            price: 120.00,
            size: 'large',
            stockLevel: 200,
        });

        expect(updateResponse.body).toBeDefined();
        expect(updateResponse.status).toBe(NOT_FOUND);
        expect(updateResponse.body.message).toBe('product with SKU:inexistent-sku not exists.');
    });

    test('This should return product not found', async () => {
        expect.assertions(3);
        const response = await request(server).get(`${endpointToCall}/inexistent-sku`);
        expect(response.status).toBe(NOT_FOUND);
        expect(response.body).toBeDefined();
        expect(response.body.message).toBe('product inexistent-sku not found.');
    });

    test('This should not delete cause sku not exists', async () => {
        expect.assertions(3);
        const response = await request(server).delete(`${endpointToCall}/inexistent-sku`);
        expect(response.status).toBe(NOT_FOUND);
        expect(response.body).toBeDefined();
        expect(response.body.message).toBe('product inexistent-sku not found.');
    });
});