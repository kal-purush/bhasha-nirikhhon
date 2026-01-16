import request from 'supertest';

import createApp from '../../app';
// import bets from '../../routes/bets';

describe('GET /api/bets', () => {
  let api = null;
  beforeEach(() => {
    const app = createApp();
    console.log('app', app);
    api = request(app);
    console.log('api', api);
  });

  it('should return default paginated bets when no query parameters', async () => {
    const response = await api
      .get('/api/bets')
      .expect('Content-Type', /json/)
      .expect(200);

    expect(response.body).toHaveProperty('bets');
    expect(response.body.bets).toHaveLength(10); // Asumiendo que hay suficientes apuestas
    expect(response.body).toHaveProperty('total');
    expect(response.body).toHaveProperty('limit', 10);
    expect(response.body).toHaveProperty('page', 1);
  });

  it('should handle valid pagination parameters', async () => {
    const response = await api
      .get('/api/bets?limit=5&page=2')
      .expect('Content-Type', /json/)
      .expect(200);

    expect(response.body.bets).toHaveLength(5);
    expect(response.body).toHaveProperty('page', 2);
  });

  it('should return error for invalid pagination parameters', async () => {
    await api
      .get('/api/bets?limit=-1&page=abc')
      .expect('Content-Type', /json/)
      .expect(400);
  });
});