import chai, { expect } from 'chai';
import chaiHttp from 'chai-http';

import app from '../index';

chai.use(chaiHttp);

/**
 * Test to get all users
 * TODO dynamically get tokens for request
 */
describe('USERS', () => {
  describe('GET/ users', () => {
    it('should return an array of users', done => {
      chai
        .request(app)
        .get('/users')
        .set(
          'authorization',
          'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VybmFtZSI6InRlc3RBZG1pbiIsImlkIjoiYTI5YzY0ZTctNjFkZC00YmQzLWE3ZTAtZjFkZjRmYTJmODNjIiwiaWF0IjoxNTg0NjEwODY0LCJleHAiOjE1ODQ2MTA5MjR9.qtUzmkw2W5INU6hSQk0Bd6HKIVSSe2khBualFW3r2GY'
        )
        .end((err, res) => {
          if (err) done(err);
          expect(res).to.have.status(200);
          expect(res).to.be.an('object');
          expect(res.body).to.be.an('array');
          done();
        });
    });
  });
});