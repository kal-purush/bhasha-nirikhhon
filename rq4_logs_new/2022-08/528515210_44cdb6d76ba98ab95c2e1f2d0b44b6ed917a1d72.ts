import * as sinon from 'sinon';
import * as chai from 'chai';
// @ts-ignore
import chaiHttp = require('chai-http');
import IUser from '../interfaces/IUser';

import { app } from '../app';
import User from '../database/models/User';
import jwtService from '../services/jwtService';
import * as bcrypt from 'bcryptjs';
import LoginService from '../services/loginService';


chai.use(chaiHttp);

const { expect } = chai;

const userMock: IUser = {
  id: 1,
  username: 'test',
  role: 'any',
  email: 'test@test.com',
  password: '123456',
}

const loginMock = {
  email: 'test@test.com',
  password: '123456',
}

const badLoginMock = {
  email: null,
  password: '123456',
}

describe('/login', () => {
  afterEach(() => {
    sinon.restore();
  })

  it('should return a token in case of success', async () => {
    sinon.stub(User, 'findOne').resolves(userMock as User);
    sinon.stub(bcrypt, 'compareSync').resolves(true);

    const response = await chai.request(app).post('/login').send(loginMock);
    console.log(response.body)
    expect(response.body).to.have.key('token');
  })

  it('should return an error if email is not send', async () => {
    const response = await chai.request(app).post('/login').send(badLoginMock);
    expect(response.body).to.be.deep.eq({ message: 'All fields must be filled' });
  })
})

describe('/login/validate', () => {
  afterEach(() => {
    sinon.restore();
  })

  it('should return a role if a valid token is send', async () => {
    sinon.stub(jwtService,  'verify').returns('test@test.com')
    sinon.stub(LoginService, 'getByEmail').resolves(userMock);

    const response = await chai.request(app).get('/login/validate').set({ "Authorization": `token` });

    expect(response.body).to.be.deep.eq({role: 'any'});
  })
})