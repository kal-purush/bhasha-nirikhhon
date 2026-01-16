import * as sinon from 'sinon';
import * as chai from 'chai';
// @ts-ignore
import chaiHttp = require('chai-http');
import IUser from '../interfaces/IUser';

import { app } from '../app';
import User from '../database/models/User'

chai.use(chaiHttp);

const { expect } = chai;

const userMock: IUser = {
  id: 1,
  username: 'test',
  role: 'any',
  email: 'test@test.com',
  password: '123456',
}

describe('Users', () => {
  afterEach(() => {
    sinon.restore();
  })

  it('should return a token in case of success', async () => {
    sinon.stub(User, 'findOne').resolves(userMock as User);

    const response = await chai.request(app).post('/login');
    expect(response.body).to.have.key('token');
  })
})