import { User } from '../user';
import * as fs from 'fs';
import { UserService } from '../user.service';
import { UserJSONService } from '../user.json-service';

describe('UserJSONService', () => {
    let sut: UserJSONService;

    beforeEach(() => {
        sut = new UserJSONService();
        jest.resetAllMocks();
    });
});