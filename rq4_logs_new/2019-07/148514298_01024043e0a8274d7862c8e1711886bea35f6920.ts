import { PasswordHelper } from '../src/password.helper';
import * as prompt from 'prompt';
import { exit } from 'shelljs';
import { SignOptions } from 'jsonwebtoken';
const JWT = require('jsonwebtoken');
require('dotenv').config();

var prompt_attributes = [
    {
        name: 'username',
        initial: 'serverAccess'
    },
    { name: 'firstName' },
    { name: 'lastName' },

    { name: 'issuer', message: 'JWT Issuer', initial: process.env.JWT_ISSUER },
    { name: 'expires (10y)', message: 'JWT Expires', initial: '10y' }
    
];

prompt.start();

prompt.get(prompt_attributes, function (err, result) {
    console.log(`Command-line input received: ${JSON.stringify(result)}`);
    console.log(`Command-line input received: ${process.env.JWT_ISSUER}`);

    var payload = {
        roles: 'admin',
        firstName: result.firstname,
        lastName: result.lastname,
        email: result.username
      };
  
      var options: SignOptions = {
        issuer: result.issuer,
        subject: result.username,
        expiresIn: result.expires,
        algorithm: 'HS256'
      };

      let token = JWT.sign(payload, process.env.JWT_PRIVATE_KEY, options);

    console.log(`token:${token}`);
    exit(1);
  });