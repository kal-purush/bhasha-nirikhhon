import { EventBus } from '../../../../Shared/domain/EventBus';
import { Nullable } from '../../../../Shared/domain/Nullable';
import { AccountId } from '../../../Shared/domain/Account/AccountId';
import TransactionAmount from '../../../Transaction/domain/TransactionAmount';
import Account from '../../domain/Account';
import AccountRepository from '../../domain/AccountRepository';

export class AccountAmountIncrementer {
    constructor(private repository: AccountRepository, private bus: EventBus) {}
    async run(accountId: AccountId, hasParents: boolean, amount: TransactionAmount, paid: boolean) {

        if (!hasParents) {
            const account:Nullable<Account> = await this.repository.search(accountId);
            if (account){
                account.modifyBalance(amount, paid);
                this.repository.update(accountId, account);
                this.bus.publish(account.pullDomainEvents());
            }
        }
    }
}