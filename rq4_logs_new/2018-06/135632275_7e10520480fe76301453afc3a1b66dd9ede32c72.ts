import { AccountService } from './AccountService';
import { AccountRepositoryMock } from '../../spi/AccountRepository/__mock__/AccountRepositoryMock';

describe('AccountService', () => {
    describe('get user accounts', () => {
        it('should throw an error when providing no user', () => {
            const accountService = new AccountService(new AccountRepositoryMock());

            expect(() => {
                accountService.getAllUserAccounts('');
            }).toThrowError('Invalid user id');
        });
    });
});