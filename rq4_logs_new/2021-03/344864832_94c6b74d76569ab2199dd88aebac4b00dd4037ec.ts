import mongoose from 'mongoose';

import * as dbHandler from './db-handler';
import {userService} from '../services/userService'
import {UserModel} from '../models/User'

/**
 * Connect to a new in-memory database before running any tests.
 */
beforeAll(async () => await dbHandler.connect());

/**
 * Clear all test data after every test.
 */
afterEach(async () => await dbHandler.clearDatabase());

/**
 * Remove and close the db and server.
 */
afterAll(async () => await dbHandler.closeDatabase());

describe('user ', () => {
    it('can be created correctly', async () => {
        expect(async () => await userService.createUser(user as UserModel))
            .not
            .toThrow();
    });
});

const user =  {
    name: "momo",
    email: "test@mailinator.com",
    password: "supersecretpassword",
}