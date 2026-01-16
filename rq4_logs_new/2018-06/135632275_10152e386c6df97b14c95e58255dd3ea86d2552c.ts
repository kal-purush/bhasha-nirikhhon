import { IAccountService } from '../api/IAccountService';
import { IAccountRepository } from '../spi/IAccountRepository';

import { User } from '../../user/entities/User';
import { IUserRepository } from '../../user/spi/IUserRepository';

class AccountService implements IAccountService {
    private accountRepository: IAccountRepository;
    private userRepository: IUserRepository;
    
    constructor(accountRepository: IAccountRepository) {
        this.accountRepository = accountRepository;
    }

    getAllUserAccounts(userId: string) {
        if (!userId) {
            // TODO : Throw InvalidUserId exception
            throw new Error('InvalidUserId');
        }

        const user = this.userRepository.findUserById(userId);
        if (!user || !user.isValid()) {
            // TODO : Thow UserNotFound exception
            throw new Error('UserNotFound');
        }

        return this.accountRepository.findUserAccounts(user);
    }
};