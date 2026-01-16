import { Test, TestingModule } from '@nestjs/testing';
import { UserService } from './user.service';
import { HttpModule, HttpService } from '@nestjs/axios';
import { of, throwError } from 'rxjs';
import { ConfigModule } from '@nestjs/config';
import { AxiosResponse } from 'axios';
import { HttpException, HttpStatus } from '@nestjs/common';
import { User } from './user.type';

describe('UserService', () => {
  let service: UserService;
  let httpService: HttpService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      imports: [HttpModule, ConfigModule],
      providers: [UserService],
    }).compile();

    service = module.get<UserService>(UserService);
    httpService = module.get<HttpService>(HttpService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  it('getUserById should return user data', async () => {
    const userId = '123';
    const expectedResult: User = {
      userName: 'Test User',
      userEmail: 'test@example.com',
      auth0_user_id: 'auth0|123',
      registeredAt: new Date(),
      lastLogin: new Date(),
      mobile: '123-456-7890',
      status: 'Married',
      dateOfBirth: new Date('1990-01-01'),
      address: {
        city: 'City',
        street: 'Street',
        num: 123,
      },
    };

    const axiosResponse: AxiosResponse<User> = {
      data: expectedResult,
      status: 200,
      statusText: 'OK',
      headers: {},
      config: {},
    };

    jest.spyOn(httpService, 'get').mockImplementation(() => of(axiosResponse));

    const result = await service.getUserById(userId);
    expect(result).toEqual(expectedResult);
  });

  it('getUsersByBusinessId should return user list', async () => {
    const businessId = '456';
    const expectedResult: User[] = [{
      userName: 'Test User',
      userEmail: 'test@example.com',
      auth0_user_id: 'auth0|123',
      registeredAt: new Date(),
      lastLogin: new Date(),
      mobile: '123-456-7890',
      status: 'Married',
      dateOfBirth: new Date('1990-01-01'),
      address: {
        city: 'City',
        street: 'Street',
        num: 123,
      },
    }];

    const axiosResponse: AxiosResponse<User[]> = {
      data: expectedResult,
      status: 200,
      statusText: 'OK',
      headers: {},
      config: {},
    };

    jest.spyOn(httpService, 'get').mockImplementation(() => of(axiosResponse));

    const result = await service.getUsersByBusinessId(businessId);
    expect(result).toEqual(expectedResult);
  });

  it('should handle errors from HTTP service', async () => {
    const userId = '123';

    jest.spyOn(httpService, 'get').mockImplementation(() => throwError(new Error('Error')));

    await expect(service.getUserById(userId)).rejects.toThrow(HttpException);
    await expect(service.getUsersByBusinessId('456')).rejects.toThrow(HttpException);
  });
});