// require the Koa server
// require supertest
import request from 'supertest';
import server from '../../src/index';
// close the server after each test
afterEach(() => {
    server.close();
});

describe("routes: index", () => {
    test("should respond as expected", async () => {
        const response = await request(server).get("/api/user");
        expect(response.status).toEqual(200);        
    });
});