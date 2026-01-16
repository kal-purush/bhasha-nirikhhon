import { User } from "../../entity/user/User";
import { PasswordEncryptorInterface } from "../../lib/passwordEncryptor/PasswordEncryptorInterface";
import { UserRepositoryInterface } from "../../repository/user/UserRepositoryInterface";
import { CreateUserRequestDTO } from "../createUser/createUserRequestDTO";

class UpdateUserUseCase {
    constructor(
        private userRepository: UserRepositoryInterface,
        private passwordEncryptor: PasswordEncryptorInterface
        ) {}
    async execute(id: string, user: CreateUserRequestDTO): Promise<void> {
        if(!this.userRepository) {
            throw new Error('User repository was not specified');
        }
        if(!id) {
            throw new Error('Id was not specified');
        }
        if(typeof(id) !== 'string') {
            throw new Error('Invalid id type');
        }
        if(!user) {
            throw new Error('User was not specified');
        }
        if(typeof(user) !== 'object') {
            throw new Error('Invalid user type');
        }
        const { name, email, password } = user;
        const encryptedPassword = await this.passwordEncryptor.execute(password);
        const userProps = {
            name,
            email,
            encryptedPassword
        }
        const updatedUser = new User(userProps);
        this.userRepository.update(id, updatedUser);
    }
}

export {
    UpdateUserUseCase
}