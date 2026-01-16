import { Account } from './Account';

describe('Account entity', () => {
    describe('validity', () => {
        let account: Account;
        
        beforeEach(() => {
            account = new Account();
        });
        
        test('should be valid when providing an id and a name', () => {
            account.accountId = 'ACCOUNT_ID';
            account.name = 'ACCOUNT_NAME';
            expect(account.isValid()).toBeTruthy();
        });

        test('should be invalid when providing no id', () => {            
            account.name = 'ACCOUNT_NAME';
            expect(account.isValid()).toBeFalsy();
        });

        test('should be invalid when providing no name', () => {            
            account.accountId = 'ACCOUNT_ID';
            expect(account.isValid()).toBeFalsy();
        });
    });
});