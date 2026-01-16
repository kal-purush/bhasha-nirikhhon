/* eslint-disable prettier/prettier */
import { Test, TestingModule } from '@nestjs/testing';
import { UsersController } from './users.controller';
import { UsersService } from './users.service';
import { User } from './user.entity';

describe('UsersController', () => {
  let usersController: UsersController;
  let usersService: jest.Mocked<UsersService>;

  beforeEach(async () => {
    usersService = {
      findOne: jest.fn(),
      findByEmail: jest.fn(),
      findAll: jest.fn(),
    } as any;

    const module: TestingModule = await Test.createTestingModule({
      controllers: [UsersController],
      providers: [{ provide: UsersService, useValue: usersService }],
    }).compile();

    usersController = module.get<UsersController>(UsersController);
  });

  it('should return a specific user', async () => {
    const userId = '1'

    const user: User = {
      id: userId,
      name: 'John',
      email: 'john@example.com',
      password: '123',
      array_of_groups: '',
      validatePassword: async (password: string) => true,
    };
    usersService.findOne.mockResolvedValue(user);

    expect(await usersController.findOne(userId)).toEqual(user);
  });

  it('should return all users', async () => {
    const users: User[] = [
      {
        id: '1',
        name: 'John',
        email: 'john@example.com',
        password: '123',
        array_of_groups: '',
        validatePassword: async (password) => true,
      },
      {
        id: '2',
        name: 'joseph',
        email: 'joseph@example.com',
        password: '321',
        array_of_groups: '',
        validatePassword: async (password) => true,
      },
    ];
    usersService.findAll.mockResolvedValue(users);

    const usersWithoutPassword = users.map((user) => {
      delete user.password;
      return user;
    });

    expect(await usersController.findAll()).toEqual(usersWithoutPassword);
  });

  it('should return a specific user by email', async () => {
    const email = 'ian@example.com';

    const user: User = {
      id: '1',
      name: 'Ian',
      email: email,
      password: '123',
      array_of_groups: '',
      validatePassword: async (password) => true,
    };
    usersService.findByEmail.mockResolvedValue(user);

    // remove password from user
    delete user.password;

    expect(await usersController.findByEmail(email)).toEqual(user);
  });
});