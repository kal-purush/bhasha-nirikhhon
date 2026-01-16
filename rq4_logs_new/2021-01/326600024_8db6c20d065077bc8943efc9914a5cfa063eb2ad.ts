import chai from 'chai';
import chaiHttp from 'chai-http';

import server from '../src/index';

const should = chai.should();
chai.use(chaiHttp);

describe('GET /vendors', () => {
  it('should test', (done) => {
    chai
      .request(server.callback())
      .get('/vendors')
      .end((err, res) => {
        should.not.exist(err);
        res.status.should.eql(200);
        done();
      });
  });
});

describe('GET /vendors/:id', () => {
  it('should test', (done) => {
    chai
      .request(server.callback())
      .get('/vendors/1')
      .end((err, res) => {
        should.not.exist(err);
        res.status.should.eql(200);
        done();
      });
  });
});

describe('POST /vendors', () => {
  it('should test', (done) => {
    chai
      .request(server.callback())
      .post('/vendors')
      .end((err, res) => {
        should.not.exist(err);
        res.status.should.eql(201);
        done();
      });
  });
});

describe('PUT /vendors/:id', () => {
  it('should test', (done) => {
    chai
      .request(server.callback())
      .put('/vendors/1')
      .end((err, res) => {
        should.not.exist(err);
        res.status.should.eql(204);
        done();
      });
  });
});