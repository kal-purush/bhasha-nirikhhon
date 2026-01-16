import request from 'supertest';
import express, {Application} from "express";
import {register} from "../../routes/routes";

let application: Application;

beforeAll(() => {
    application = express();
    register(application);
})

describe('GET /', () => {

    describe('send request', () => {
        // should response with a 200 status code and text/html content
        test('should response with a 200 status code',  async() => {
            const response = await request(application).get(`/`);
            expect(response.statusCode).toBe(200);
            expect(response.headers['content-type']).toEqual(expect.stringContaining('text/html'));
        })

    })

});