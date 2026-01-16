import { DeleteUserUseCase } from "./deleteUserUseCase"
import { inMemoryUserRepository } from "../../repository/user/implementation/InMemoryUserRepository";
import { DeleteUserController } from "./deleteUserController";

const deleteUserUseCase = new DeleteUserUseCase(
    inMemoryUserRepository
);

const deleteUserController = new DeleteUserController(
    deleteUserUseCase
);

export {
    deleteUserUseCase,
    deleteUserController
};