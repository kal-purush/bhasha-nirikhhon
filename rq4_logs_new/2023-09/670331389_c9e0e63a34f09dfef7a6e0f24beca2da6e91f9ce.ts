import { httpServer, testConfig } from '../test-config';
import { usersTestManager } from '../users/users-test-manager';
import { HttpStatus } from '@nestjs/common';
import request from 'supertest';

describe('devicesTests', () => {
  testConfig();

  let userDataOne;

  it('should create two user ', async () => {
    userDataOne = {
      login: 'Asta',
      password: 'Asta1999',
      email: 'Asta@gmail.com',
    };
  });
});