import { Account } from '../../entities/Account/Account';

import { IAccountService } from "../../../api/AccountService/IAccountService";

export class AccountServiceMock implements IAccountService {
    getAllUserAccounts = jest.fn((userId: string): Account[] => {
        return null;
    });

    getAccount = jest.fn((accountId: string, userId: string): Account => {
        return null;
    });

    createAccount = jest.fn((account: Account, userId: string): Account => {
        return null;
    });

    modifyAccount = jest.fn((account: Account, userId: string): Account => {
        return null;
    });

    removeAccount = jest.fn((accountId: string, userId: string): boolean => {
        return null;
    });

};