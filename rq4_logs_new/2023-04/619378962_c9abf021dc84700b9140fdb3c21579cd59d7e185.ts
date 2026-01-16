import { UserRepositoryInterface } from "../../repository/user/UserRepositoryInterface";

class DeleteUserUseCase {
    constructor(
        private userRepository: UserRepositoryInterface
    ) {}
    async execute(id: string): Promise<void> {
        if(!this.userRepository) {
            throw new Error('User repository not specified');
        }
        if(!id) {
            throw new Error('Id not specified');
        }
        if(typeof(id) !== 'string') {
            throw new Error('Invalid id');
        }
        const targetUserExists = await this.userRepository.getUserById(id);
        if(!targetUserExists) {
            throw new Error('User not found');
        }
        await this.userRepository.delete(id);
    }
}

export {
    DeleteUserUseCase
}