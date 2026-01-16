import { UpdateUserUseCase } from "./updateUserUseCase"
import { inMemoryUserRepository } from "../../repository/user/implementation/InMemoryUserRepository";
import { UpdateUserController } from "./updateUserController";
import { PasswordEncryptorInterface } from "../../lib/passwordEncryptor/PasswordEncryptorInterface";
import { BcryptPasswordEncryptor } from "../../lib/passwordEncryptor/BcryptPasswordEncryptor";

const passwordEncryptor: PasswordEncryptorInterface = new BcryptPasswordEncryptor();

const updateUserUseCase = new UpdateUserUseCase(
    inMemoryUserRepository,
    passwordEncryptor
);

const updateUserController = new UpdateUserController(
    updateUserUseCase
);

export {
    updateUserUseCase,
    updateUserController
};