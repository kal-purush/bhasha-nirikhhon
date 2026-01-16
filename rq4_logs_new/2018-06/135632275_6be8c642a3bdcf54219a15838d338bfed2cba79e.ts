import { IUserService } from '../../api/UserService/IUserService';

import { IUserRepository } from '../../spi/UserRepository/IUserRepository';

import { User } from '../entities/User/User';

export class UserService implements IUserService {
    private userRepository: IUserRepository;

    constructor(userRepository: IUserRepository) {
        this.userRepository = userRepository;
    };

    searchUser(query: string): Array<User> {
        if (!query) {
            throw new Error('Invalid query');
        }

        return this.userRepository.searchUsers(query);
    };

    createUser(user: User): User {
        if (!user || !user.isValid()) {
            throw new Error('Invalid user');
        }

        return this.userRepository.createUser(user);
    };

    removeUser(userId: string): boolean {
        if (!userId) {
            throw new Error('Invalid user id');
        }

        return this.userRepository.deleteUser(userId);
    };
};