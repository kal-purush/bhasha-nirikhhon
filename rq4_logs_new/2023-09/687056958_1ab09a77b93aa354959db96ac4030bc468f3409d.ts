import { Request, Response } from 'express';
import { createMocks } from 'node-mocks-http'
import usersController from '../src/controllers/usersController'
import * as userService from '../src/services/userService';

import { allUsers } from './mocks/usersMocks';

jest.mock('../src/services/userService');

  describe('list users', () => {
    beforeEach(() => {
      jest.clearAllMocks()
    })
  
    afterAll(() => {
      jest.clearAllMocks()
    })

    it('List all users', async() => {
      const mockList = jest.spyOn(userService, 'list');
      mockList.mockResolvedValue(allUsers);

      const { req, res } = createMocks<Request, Response>();
        
      await usersController.list(req, res);

      expect(mockList).toHaveBeenCalled();
      expect(res._getStatusCode()).toBe(200);
      expect(res._getJSONData()).toEqual(allUsers);
    })

  })