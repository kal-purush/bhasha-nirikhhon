import UserController from '../controllers/userController';
import express from 'express';

jest.mock('bcrypt')
const bcryptMock = require('bcrypt');
jest.mock('jsonwebtoken')
const jwtMock = require('jsonwebtoken');
jest.mock('crypto')
const cryptoMock = require('crypto');
jest.mock('../services/userService');
const userServiceMock = require('../services/userService');
jest.mock('../services/saltService');
const saltServiceMock = require('../services/saltService');

describe('user controller', () => {
    bcryptMock.genSalt = jest.fn().mockReturnValue(Promise.resolve('salt'));
    bcryptMock.hash = jest.fn().mockReturnValue(Promise.resolve('hash'));
    jwtMock.sign = jest.fn().mockReturnValue('token');
    cryptoMock.randomBytes = jest.fn().mockReturnValue({
        toString: jest.fn().mockReturnValue('secret')
    });
    saltServiceMock.saltService.createSaltForUser = jest.fn().mockReturnValue(Promise.resolve());
        
    it('signup', async () => {
        userServiceMock.userService.findUserByQuery = jest.fn().mockReturnValue(Promise.resolve(null));
        userServiceMock.userService.createUser = jest.fn().mockReturnValue(Promise.resolve({
            _id: 'newuserId'
        }));
        const body = {
            name: "yoda",
            email: "yoda@mail.com",
            password: "Yoda1234",
        }
        const req = {
            body
        } as express.Request;
        const res = {
           status: jest.fn().mockReturnValue({
               send: jest.fn()
           }),
           json: jest.fn()
        } as unknown as express.Response;

        await UserController.signup(req, res);
        expect(userServiceMock.userService.findUserByQuery).toBeCalledWith({email: body.email});
        expect(bcryptMock.genSalt).toHaveBeenCalled();
        expect(bcryptMock.hash).toHaveBeenCalledWith(body.password, 'salt');
        expect(userServiceMock.userService.createUser).toHaveBeenCalledWith({...body, password: 'hash'});
        expect(saltServiceMock.saltService.createSaltForUser).toHaveBeenCalledWith("newuserId", "salt");
        expect(res.json).toHaveBeenCalledWith({...body, password: '', token: 'token'}); 
    });
});