import { IAccountRepository } from '../../../domain/account/spi/AccountRepository/IAccountRepository';

import { Account } from '../../../domain/account/core/entities/Account/Account';
import { User } from '../../../domain/user/core/entities/User/User';

export class InMemoryAccountRepository implements IAccountRepository {
    private accounts: Array<Account>;

    constructor() {
        this.accounts = [];
    }

    findUserAccounts(user: User): Account[] {
        return this.accounts.filter((account: Account) => {
            return user &&
                account &&
                account.users.find((accountUser: User): boolean => {
                    return accountUser && accountUser.userId === user.userId;
                });
        });
    }

    findAccountById(accountId: string): Account {
        if (!accountId) {
            return null;
        }
        return this.accounts.find((account: Account) => {
            return account && account.accountId === accountId;
        });
    }

    createAccount(account: Account): Account {
        this.accounts.push(account);
        return account;
    }

    updateAccount(updatedAccount: Account): Account {
        if (!updatedAccount || !updatedAccount.accountId) {
            throw new Error('Invalid account');
        }

        const accountIndex = this.accounts.findIndex((account: Account) => {
            return account && account.accountId === updatedAccount.accountId;
        });

        if (accountIndex < 0) {
            throw new Error('Account not found');
        }

        this.accounts[accountIndex] = updatedAccount;

        return updatedAccount;
    }

    deleteAccount(accountId: string): boolean {
        let success: boolean = false;

        const accountIndex = this.accounts.findIndex((account: Account): boolean => {
            return account && account.accountId === accountId;
        });
        if (accountIndex >= 0) {
            success = !!this.accounts.splice(accountIndex, 1);
        }

        return success;
    }
};