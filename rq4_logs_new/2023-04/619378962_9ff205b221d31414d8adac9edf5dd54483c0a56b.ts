import { it, expect, describe } from 'vitest';
import { DeleteUserUseCase } from './deleteUserUseCase';
import { InMemoryUserRepository } from '../../repository/user/implementation/InMemoryUserRepository';
import { CreateUserUseCase } from '../createUser/createUserUseCase';
import { BcryptPasswordEncryptor } from '../../lib/passwordEncryptor/BcryptPasswordEncryptor';
import { GetAllUsersUseCase } from '../getAllUsers/getAllUsersUseCase';
import { User } from '../../entity/user/User';

const makeSut = () => {
    const userRepository = new InMemoryUserRepository();
    const passwordEncryptor = new BcryptPasswordEncryptor();
    const createUserUseCase = new CreateUserUseCase(userRepository, passwordEncryptor);
    const getAllUsersUseCase = new GetAllUsersUseCase(userRepository);
    const sut = new DeleteUserUseCase(userRepository);
    return {
        userRepository,
        createUserUseCase,
        getAllUsersUseCase,
        sut
    };
};

const createUser = async (createUserUseCase): Promise<void> => {
    const user = {
        name: 'Marcos',
        email: 'marcos@example.com',
        password: 'somePassword'
    };
    await createUserUseCase.execute(user);
};

const getAllUsers = async (getAllUsersUseCase): Promise<User[] | []> => {
    const users = await getAllUsersUseCase.execute();
    return users;
}

describe('Delete User Use Case', () => {
    it('should delete an user', async () => {
        const { createUserUseCase, getAllUsersUseCase, sut } = makeSut();
        await createUser(createUserUseCase);
        const users = await getAllUsers(getAllUsersUseCase);
        const id = users[0].getId();

        await sut.execute(id);
        
        expect(users).not.toContainEqual(users[0]);
    });
});