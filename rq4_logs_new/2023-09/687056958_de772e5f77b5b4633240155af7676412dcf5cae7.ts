import { Request, Response } from 'express';
import chai, { expect } from 'chai'
import sinon from 'sinon'
import sinonChai from 'sinon-chai' 
import * as userService from '../src/services/userService';
import { loginBody, validLoginUser, validToken } from './mocks/usersMocks';
import loginController from '../src/controllers/loginController'
import * as authToken from '../src/validators/jwt/createToken'

chai.use(sinonChai)

describe.only('User login', () => {

  afterEach(() => {
    sinon.reset()
  })

it('Successfully login', async() => {
  sinon.stub(userService, 'login').resolves(validLoginUser)
  sinon.stub(authToken, 'createToken').returns(validToken)

  const req = {} as Request
  const res = {} as Response

  req.body = loginBody
  res.status = sinon.stub().returns(res)
  res.json = sinon.stub().returns(res)
  
  await loginController.login(req, res)
  expect(res.status).to.have.been.calledWith(200)
  expect(res.json).to.have.been.calledWith({ acessToken: validToken})

  
})

})