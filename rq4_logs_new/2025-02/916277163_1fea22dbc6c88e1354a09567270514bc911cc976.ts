import { beforeEach, describe, expect, it, jest } from '@jest/globals';

import { Nest } from '../api';

import { EGender, ERole, EStatus } from '../shared';

import { AuthService } from './authService';

import { SignInParams, SignUpParams, UserEntity } from './interface';

describe('AuthService', () => {
  let authService: AuthService;
  let mockNest: jest.Mocked<Nest>;

  const mockUser: UserEntity = {
    id: 'eaca4c08-e62d-495a-ae1c-918199da8d52',
    cpf: '12345678909',
    role: ERole.USER,
    name: 'Test User',
    email: 'testuser@example.com',
    gender: EGender.MALE,
    status: EStatus.ACTIVE,
    whatsapp: '123456789',
    created_at: new Date('2024-09-09'),
    updated_at: new Date('2024-09-09'),
    date_of_birth: new Date('2000-01-01'),
  };
  const mockPassword: string = 'testPassword';
  const mockSignUpParams: SignUpParams = {
    cpf: mockUser.cpf,
    name: mockUser.name,
    email: mockUser.email,
    gender: mockUser.gender,
    password: mockPassword,
    whatsapp: mockUser.whatsapp,
    date_of_birth: mockUser.date_of_birth,
    password_confirmation: mockPassword,
  };
  const mockSignInParams: SignInParams = {
    email: mockUser.email,
    password: mockPassword,
  };
  const mockResponseError = { message: 'Internal Server Error' };
  const mockResponseNotFound = { message: 'Not Found' };

  beforeEach(() => {
    mockNest = {
      auth: {
        signUp: jest.fn(),
        signIn: jest.fn(),
        getOne: jest.fn(),
        me: jest.fn(),
      },
    } as unknown as jest.Mocked<Nest>;

    authService = new AuthService(mockNest);
  });

  describe('signUp', () => {
    it('should return success message when registering', async () => {
      const mockResponse = { message: 'Registration Completed Successfully!' };

      mockNest.auth.signUp.mockResolvedValue(mockResponse);

      const result = await authService.signUp(mockSignUpParams);
      expect(result).toBe(mockResponse.message);
      expect(mockNest.auth.signUp).toHaveBeenCalledTimes(1);
      expect(mockNest.auth.signUp).toHaveBeenCalledWith(mockSignUpParams);
    });

    it('should throw error when registration fails', async () => {
      mockNest.auth.signUp.mockRejectedValue(
        new Error(mockResponseError.message),
      );
      await expect(authService.signUp(mockSignUpParams)).rejects.toThrow(
        mockResponseError.message,
      );

      expect(mockNest.auth.signUp).toHaveBeenCalledTimes(1);
      expect(mockNest.auth.signUp).toHaveBeenCalledWith(mockSignUpParams);
    });
  });

  describe('signIn', () => {
    it('should return token when logging in', async () => {
      const mockResponse = { token: 'abc123' };

      mockNest.auth.signIn.mockResolvedValue(mockResponse);

      const result = await authService.signIn(mockSignInParams);
      expect(result).toBe(mockResponse.token);
      expect(mockNest.auth.signIn).toHaveBeenCalledTimes(1);
      expect(mockNest.auth.signIn).toHaveBeenCalledWith(mockSignInParams);
    });

    it('should throw error when login fails', async () => {
      mockNest.auth.signIn.mockRejectedValue(
        new Error(mockResponseError.message),
      );
      await expect(authService.signIn(mockSignInParams)).rejects.toThrow(
        mockResponseError.message,
      );

      expect(mockNest.auth.signIn).toHaveBeenCalledTimes(1);
      expect(mockNest.auth.signIn).toHaveBeenCalledWith(mockSignInParams);
    });
  });

  describe('get', () => {
    it('should return a user when searching by id', async () => {
      mockNest.auth.getOne.mockResolvedValue(mockUser);

      const result = await authService.get(mockUser.id);
      expect(result).toEqual(mockUser);
      expect(mockNest.auth.getOne).toHaveBeenCalledTimes(1);
      expect(mockNest.auth.getOne).toHaveBeenCalledWith(mockUser.id);
    });

    it('should throw error when userid not found', async () => {
      mockNest.auth.getOne.mockRejectedValue(
        new Error(mockResponseNotFound.message),
      );

      await expect(authService.get(mockUser.id)).rejects.toThrow(
        mockResponseNotFound.message,
      );
      expect(mockNest.auth.getOne).toHaveBeenCalledTimes(1);
      expect(mockNest.auth.getOne).toHaveBeenCalledWith(mockUser.id);
    });
  });

  describe('me', () => {
    it('should return the current user', async () => {
      mockNest.auth.me.mockResolvedValue(mockUser);

      const result = await authService.me();
      expect(result).toEqual(mockUser);
      expect(mockNest.auth.me).toHaveBeenCalledTimes(1);
    });

    it('should throw error when failing to fetch current user', async () => {
      mockNest.auth.me.mockRejectedValue(new Error(mockResponseError.message));

      await expect(authService.me()).rejects.toThrow(mockResponseError.message);
      expect(mockNest.auth.me).toHaveBeenCalledTimes(1);
    });
  });
});