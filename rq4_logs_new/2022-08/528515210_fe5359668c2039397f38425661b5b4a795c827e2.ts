import * as sinon from 'sinon';
import * as chai from 'chai';
// @ts-ignore
import chaiHttp = require('chai-http');

import { app } from '../app';
import Team from '../database/models/Team';
import { teamMock } from './mocks/teamMocks';

chai.use(chaiHttp);

const { expect } = chai;

describe('/teams', () => {

  it('should return status 200 and an array of teams at the route get /teams', async () => {
    sinon.stub(Team, "findAll").resolves([teamMock as Team])

    const response = await chai.request(app).get('/teams');
    expect(response.status).to.equal(200);
    expect(response.body).to.be.an('array');
    expect(response.body).to.be.deep.equal([teamMock]);

    sinon.restore();
  })
})

describe('/teams/:id', () => {
  afterEach(() => {
    sinon.restore();
  })

  it('should return status 200 and an object with keys "id" and "teamName"', async () => {
    sinon.stub(Team, 'findByPk').resolves(teamMock as Team)

    const res = await chai.request(app).get('/teams/:id');
    expect(res.status).to.equal(200);
    expect(res.body).to.have.all.keys("id", "teamName")
  })

  it('should return status 404 and the message "ID not found" if the id is not found', async () => {
    sinon.stub(Team, 'findByPk').resolves(null)

    const res = await chai.request(app).get('/teams/:id');
    expect(res.status).to.equal(404);
    expect(res.body.message).to.be.eq('ID not found')
  })
})