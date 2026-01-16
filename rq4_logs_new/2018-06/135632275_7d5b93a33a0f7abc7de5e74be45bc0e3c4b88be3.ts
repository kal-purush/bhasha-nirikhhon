import { Transaction } from './Transaction';

describe('Transaction entity', () => {
    describe('validity', () => {
        let transaction: Transaction;

        const ACCOUNT_ID: string = 'ACCOUNT_ID';
        const USER_ID: string = 'USER_ID';
        const TRANSACTION_NAME: string = 'TRANSACTION_NAME';
        const TRANSACTION_AMOUNT: number = -42;
        const TRANSACTION_DATE: Date = new Date();

        beforeEach(() => {
            transaction = new Transaction();
            transaction.accountId = ACCOUNT_ID;
            transaction.userId = USER_ID;
            transaction.name = TRANSACTION_NAME;
            transaction.amount = TRANSACTION_AMOUNT;
            transaction.date = TRANSACTION_DATE;
        });

        test('should be invalid when providing no account id', () => {
            transaction.accountId = null;           
            expect(transaction.isValid()).toBeFalsy();
        });

        test('should be invalid when providing no user id', () => {
            transaction.userId = null;           
            expect(transaction.isValid()).toBeFalsy();
        });

        test('should be invalid when providing no name', () => {
            transaction.name = null;           
            expect(transaction.isValid()).toBeFalsy();
        });

        test('should be invalid when providing no amount', () => {
            transaction.amount = null;           
            expect(transaction.isValid()).toBeFalsy();
        });

        test('should be invalid when providing no date', () => {
            transaction.date = null;           
            expect(transaction.isValid()).toBeFalsy();
        });
        
        test('should be valid when providing an account name', () => {
            expect(transaction.isValid()).toBeTruthy();
        });
    });
});