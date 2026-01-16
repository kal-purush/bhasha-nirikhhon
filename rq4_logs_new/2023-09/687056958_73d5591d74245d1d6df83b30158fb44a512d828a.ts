import { Request, Response } from 'express';
import { createMocks } from 'node-mocks-http'

import loginController from '../src/controllers/loginController'
import * as userService from '../src/services/userService';
import { validLoginUser } from './mocks/usersMocks';
import crypto from '../src/helper/cryptoPrassword'

describe.only('User login', () => {
  beforeEach(() => {
    jest.clearAllMocks()
  })



  it('Successfully login', async() => {
    const mockService = jest.spyOn(userService, 'login');
    mockService.mockResolvedValue(validLoginUser);

    const mockDecrypte = jest.spyOn(crypto, 'decryptoPassword');
    mockDecrypte.mockResolvedValue(true)

    const { req, res } = createMocks<Request, Response>();
    req.body = {
      email: 'maria@email.com',
      password: '123456'
    }
      
    await loginController.login(req, res);

    expect(mockService).toHaveBeenCalled();
    expect(mockDecrypte).toHaveBeenCalled();
    expect(res._getStatusCode()).toBe(200);
    expect(res._getJSONData()).toHaveProperty('acessToken');
  })

  it('Try to login with invalid password', async() => {
    const mockService = jest.spyOn(userService, 'login');
    mockService.mockResolvedValue(validLoginUser);

    const mockDecrypte = jest.spyOn(crypto, 'decryptoPassword');
    mockDecrypte.mockResolvedValue(false)

    const { req, res } = createMocks<Request, Response>();
    req.body = {
      email: 'maria@email.com',
      password: '111111'
    }
      
    await loginController.login(req, res);

    expect(mockService).toHaveBeenCalled();
    expect(mockDecrypte).toHaveBeenCalled();
    expect(res._getStatusCode()).toBe(404);
    expect(res._getJSONData()).toHaveProperty('message');
    })
  

})