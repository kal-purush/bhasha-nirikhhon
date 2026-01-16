import Server from 'core/http/server';
import Application from 'core/boot/application';
import * as dotenv from 'dotenv-safe';
import {
    ACCEPTED,
    CONFLICT,
    CREATED, NOT_ACCEPTABLE, NOT_FOUND, NO_CONTENT, OK,
} from 'http-status';
import mongoose from 'mongoose';
import request from 'supertest';
import { IProduct } from 'interfaces/product';
import { IBasket } from 'interfaces/basket';

describe('[GET] - /baskets', () => {
    let server;
    const endpointToCall = '/baskets';

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

    test('This should insert, show and remove basket by ID', async () => {
        expect.assertions(9);
        const sku = String((Math.random() + 1).toString(36).substring(7));

        const createProduct = await request(server).post('/products').send({
            sku,
            name: sku,
            price: 100.00,
            size: 'large',
            stockLevel: 100,
        });

        const createBasket = await request(server).post(endpointToCall).send({
            items: [
                {
                    sku,
                    quantity: 2,
                },
            ],
            delivery: {
                zip_code: '1000-100',
            },
            coupon: '20DISCOUNT',
        });

        // eslint-disable-next-line no-underscore-dangle
        const retrieveResponse = await request(server).get(`${endpointToCall}/${createBasket.body._id}`);
        // eslint-disable-next-line no-underscore-dangle
        const deleteProduct = await request(server).delete(`/products/${sku}`);
        // eslint-disable-next-line no-underscore-dangle
        const deleteBasket = await request(server).delete(`${endpointToCall}/${createBasket.body._id}`);

        expect(createProduct.body).toBeDefined();
        expect(createProduct.status).toBe(CREATED);
        expect(createBasket.body).toBeDefined();
        expect(createBasket.status).toBe(CREATED);
        expect(retrieveResponse.body).toBeDefined();
        expect(retrieveResponse.body).toMatchObject({} as IBasket);
        expect(retrieveResponse.status).toBe(OK);
        expect(deleteProduct.status).toBe(NO_CONTENT);
        expect(deleteBasket.status).toBe(NO_CONTENT);
    });

    test('This should not insert basket cause stock problem', async () => {
        expect.assertions(6);
        const sku = String((Math.random() + 1).toString(36).substring(7));

        const createProduct = await request(server).post('/products').send({
            sku,
            name: sku,
            price: 100.00,
            size: 'large',
            stockLevel: 100,
        });

        const createBasket = await request(server).post(endpointToCall).send({
            items: [
                {
                    sku,
                    quantity: 200000000,
                },
            ],
            delivery: {
                zip_code: '1000-100',
            },
            coupon: '20DISCOUNT',
        });

        // eslint-disable-next-line no-underscore-dangle
        const deleteProduct = await request(server).delete(`/products/${sku}`);

        expect(createProduct.body).toBeDefined();
        expect(createProduct.status).toBe(CREATED);
        expect(createBasket.body).toBeDefined();
        expect(createBasket.body.message).toBe(`we don't have 200000000 of product with SKU:${sku} in stock.`);
        expect(createBasket.status).toBe(NOT_ACCEPTABLE);
        expect(deleteProduct.status).toBe(NO_CONTENT);
    });

    test('This should not insert cause SKU of product not exists.', async () => {
        expect.assertions(3);

        const createBasket = await request(server).post(endpointToCall).send({
            items: [
                {
                    sku: 'no-existent',
                    quantity: 2,
                },
            ],
            delivery: {
                zip_code: '1000-100',
            },
            coupon: '20DISCOUNT',
        });

        expect(createBasket.body).toBeDefined();
        expect(createBasket.body.message).toBe('product with SKU:no-existent not exists.');
        expect(createBasket.status).toBe(CONFLICT);
    });

    test('This should not retrieve cause basket ID not exists', async () => {
        expect.assertions(2);

        const retrieveBasket = await request(server).get(`${endpointToCall}/927c057f68cd59ee9a5ad4a1`);

        expect(retrieveBasket.body).toBeDefined();
        expect(retrieveBasket.status).toBe(NOT_FOUND);
    });

    test('This should not delete cause basket ID not exists', async () => {
        expect.assertions(2);

        const deleteBasket = await request(server).delete(`${endpointToCall}/927c057f68cd59ee9a5ad4a1`);

        expect(deleteBasket.body).toBeDefined();
        expect(deleteBasket.status).toBe(NOT_FOUND);
    });
});