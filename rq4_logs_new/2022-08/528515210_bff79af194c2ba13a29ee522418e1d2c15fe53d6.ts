import * as sinon from 'sinon';
import * as chai from 'chai';
// @ts-ignore
import chaiHttp = require('chai-http');

import { app } from '../app';
import Match from '../database/models/Match';
import { matchMock } from './mocks/matchMocks';

// import { Response } from 'superagent';

chai.use(chaiHttp);

const { expect } = chai;

describe('/matches', () => {
  afterEach(() => {
    sinon.restore();
  })

  it('should return status 200 and an array of matches', async () => {
    sinon.stub(Match, "findAll").resolves([matchMock as unknown as Match])

    const response = await chai.request(app).get('/matches');
    expect(response.status).to.equal(200)
    expect(response.body).to.be.an('array');
    expect(response.body).to.be.deep.equal([matchMock]);
  })
})