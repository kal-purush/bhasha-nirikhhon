const supertest = require('supertest');
const app = require('../server');
const request = supertest(app);

describe('Ranking test suite', () => {

   it('Should return statusCode 200 and body not null when ranking has been called', async () => {
      const res = await request.get('/api/ranking');
      expect(res.statusCode).toEqual(200);
      expect(res.body).not.toBeNull();
   });

});