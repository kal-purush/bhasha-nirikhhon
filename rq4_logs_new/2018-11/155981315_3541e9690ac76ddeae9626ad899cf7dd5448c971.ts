import { Test, TestingModule } from '@nestjs/testing';

import { LocalDateTime } from 'js-joda';

import { UsersController } from './users.controller';
import { CreateUserCommand } from './dto/create-user-command.dto';
import { UsersService } from './users.service';
import { User } from './domain/user.domain';

describe('Users Controller', () => {

    let module: TestingModule;
    let controller: UsersController;
    let usersService: UsersService;

    beforeAll(async () => {
        module = await Test.createTestingModule({
            controllers: [
                UsersController,
            ],
            providers: [
                UsersService,
            ]
        }).compile();
        controller = module.get<UsersController>(UsersController);
        usersService = module.get<UsersService>(UsersService);
    });

    it('should be defined', () => {
        expect(controller).toBeDefined();
    });

    describe('createUser', () => {

        const createUserCommand = {
            login: 'test',
            password: '12356',
        } as CreateUserCommand;

        const createdUser = {
            id: 13,
            login: 'test',
            password: '123456',
            creationDate: LocalDateTime.now(),
        } as User;

        it('should call service and return created user', () => {
            jest.spyOn(usersService, 'createUser').mockImplementation(() => createdUser);

            const result = controller.createUser(createUserCommand);

            expect(usersService.createUser).toHaveBeenCalledWith(createUserCommand);
            expect(result).toEqual(createdUser);
        });

    });

});