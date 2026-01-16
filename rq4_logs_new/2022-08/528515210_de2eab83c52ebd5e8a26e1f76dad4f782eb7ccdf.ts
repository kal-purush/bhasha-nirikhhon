import * as sinon from 'sinon';
import * as chai from 'chai';
// @ts-ignore
import chaiHttp = require('chai-http');

import { app } from '../app';
import LeaderboardService from '../services/leaderboardService';
import { allMatchesMock, allTeamsMock, leaderboardAwayMock, leaderboardHomeMock, leaderboardMock } from './mocks/leaderboardMocks';
import TeamService from '../services/teamService';
import MatchService from '../services/matchService';
import Match from '../database/models/Match';

chai.use(chaiHttp);

const { expect } = chai;

afterEach(() => {
  sinon.restore();
})

describe('/leaderboard/home', () => {
  it('should return status 200 and an array', async () => {
    sinon.stub(TeamService, 'getAll').resolves(allTeamsMock);
    sinon.stub(MatchService, 'getAll').resolves(allMatchesMock as unknown as Match[])

    const res = await chai.request(app).get('/leaderboard/home')
    console.log(res.body)
    expect(res.status).to.be.eq(200);
    expect(res.body).to.be.deep.eq(leaderboardHomeMock);
  })
})

describe('/leaderboard/away', () => {
  it('should return status 200 and an array', async () => {
    sinon.stub(TeamService, 'getAll').resolves(allTeamsMock);
    sinon.stub(MatchService, 'getAll').resolves(allMatchesMock as unknown as Match[])

    const res = await chai.request(app).get('/leaderboard/away')
    expect(res.status).to.be.eq(200);
    expect(res.body).to.be.deep.eq(leaderboardAwayMock);
  })
})

  describe('/leaderboard', () => {
    it('should return status 200 and an array', async () => {
      sinon.stub(TeamService, 'getAll').resolves(allTeamsMock);
      sinon.stub(LeaderboardService, 'getResults')
        .onFirstCall().resolves(leaderboardHomeMock)
        .onSecondCall().resolves(leaderboardAwayMock);

      const res = await chai.request(app).get('/leaderboard')
      expect(res.status).to.be.eq(200);
      expect(res.body).to.be.deep.eq(leaderboardMock);
    })
})