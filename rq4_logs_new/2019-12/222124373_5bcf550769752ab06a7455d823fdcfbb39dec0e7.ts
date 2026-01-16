import { expect } from 'chai';
import request from 'supertest';

import App from './../../src/app';

export default () => {
  let server = null;
  let app = null;

  before(() => {
    server = new App();
    app = server.app;

    server.listen();
  });

  after(() => {
    server.close();
  });

  describe('GET /users', () => {
    const endpoint: string = '/v1/users';

    it('should return a valid response', async () => {
      request(app)
        .get(endpoint)
        .expect(200)
        .then((res) => {
          const { body } = res;
          expect(body).to.be.an('array');
          expect(body).to.have.length(1);

          const user = body[0];
          expect(user).to.be.an('object');
          expect(Object.keys(user).length).to.equal(2);

          expect(user.id).to.equal(1);
          expect(user.name).to.equal('Martijn');
        })
        .catch((err) => expect(err).to.be.undefined);
    });
  });
};