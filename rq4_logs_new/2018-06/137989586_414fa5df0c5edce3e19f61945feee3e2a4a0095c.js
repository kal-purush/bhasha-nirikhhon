
const request = require('supertest');
const gpsApi = require('../gps-api');

describe('gps-api.js', () => {
  test('the root path should respond to GET method', async () => {
    const response = await request(gpsApi).get('/');
    expect(response.statusCode).toBe(200);
  });
});