import { UnauthorizedAccessException } from '../../../../Shared/domain/exceptions/UnauthorizedAccessException';
import Logger from '../../../../Shared/domain/Logger';
import { Nullable } from '../../../../Shared/domain/Nullable';
import { UserPetition } from '../../../Shared/domain/User/UserPetition';
import { UserType } from '../../../Shared/domain/User/UserType';
import Account from '../../../Account/domain/Account';
import AccountRepository from '../../domain/AccountRepository';

export class  MostRecentAccountFinder {

    constructor(private repository: AccountRepository, private logger: Logger) {
    }

    async run(userPetition: UserPetition): Promise<Nullable<Account[]>> {
        let accounts = await this.repository.searchNRecentAccountsFromUser(userPetition.id, 3);
        this.logger.info("User with id " + userPetition.id.value + " searched his recent accounts")
        return accounts;
    }

    hasPermission(userPetition:UserPetition):boolean{
        return userPetition.type == UserType.ADMIN;
    }

}