import { beforeEach, describe, expect, it, jest } from '@jest/globals';

import { Auth } from './auth';
import { NestModuleAbstract } from '../nestModuleAbstract';
import {ISignInParams, ISignUpParams} from './interface';
import { EGender } from '../../../shared';

jest.mock('../nestModuleAbstract'); // Mock da classe base

describe('Auth', () => {
  const mockBaseUrl = 'http://mock-base-url.com';
  const mockHeaders = { Authorization: 'Bearer test-token' };
  const mockConfig = { baseUrl: mockBaseUrl, headers: mockHeaders };

  let auth: Auth;

  beforeEach(() => {
    jest.clearAllMocks();
    auth = new Auth(mockConfig);
  });

  it('should initialize with the correct path and config', () => {
    expect(NestModuleAbstract).toHaveBeenCalledTimes(1);
    expect(NestModuleAbstract).toHaveBeenCalledWith('auth', mockConfig);
  });

  it('should call post with correct URL and body for signUp', async () => {
    const mockPost = jest
      .spyOn(NestModuleAbstract.prototype, 'post')
      .mockResolvedValue({ message: 'User registered successfully' });

    (NestModuleAbstract.prototype as any).pathUrl = 'auth';

    const mockSignUpParams: ISignUpParams = {
      cpf: '12345678909',
      name: 'Test User',
      email: 'testuser@example.com',
      gender: EGender.MALE,
      password: 'testPassword',
      whatsapp: '123456789',
      date_of_birth: new Date('2000-01-01'),
      password_confirmation: 'testPassword',
    };
    const result = await auth.signUp(mockSignUpParams);

    expect(mockPost).toHaveBeenCalledTimes(1);
    expect(mockPost).toHaveBeenCalledWith('auth/signUp', {
      body: mockSignUpParams,
    });
    expect(result).toEqual({ message: 'User registered successfully' });
  });

  it('should call post with correct URL and body for signIn', async () => {
    const mockPost = jest
      .spyOn(NestModuleAbstract.prototype, 'post')
      .mockResolvedValue({ token: 'mock-jwt-token' });

    const mockSignInParams: ISignInParams = { email: 'testUser', password: 'testPassword' };
    const result = await auth.signIn(mockSignInParams);

    expect(mockPost).toHaveBeenCalledTimes(1);
    expect(mockPost).toHaveBeenCalledWith('auth/signIn', {
      body: mockSignInParams,
    });
    expect(result).toEqual({ token: 'mock-jwt-token' });
  });

  it('should call get with correct URL for me', async () => {
    const mockGet = jest
      .spyOn(NestModuleAbstract.prototype, 'get')
      .mockResolvedValue({
        id: '1',
        username: 'testUser',
        email: 'testUser@example.com',
      });

    const result = await auth.me();

    expect(mockGet).toHaveBeenCalledTimes(1);
    expect(mockGet).toHaveBeenCalledWith('auth/me');
    expect(result).toEqual({
      id: '1',
      username: 'testUser',
      email: 'testUser@example.com',
    });
  });
});