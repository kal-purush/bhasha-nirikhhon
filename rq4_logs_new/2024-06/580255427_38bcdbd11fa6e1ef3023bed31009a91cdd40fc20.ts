import { Test, TestingModule } from '@nestjs/testing';
import { LogoutResolver } from './logout.resolver';
import { LogoutService } from './logout.service';
import { LogoutRequestInput } from '@/graphql/graphql'; // Adjust the import path as necessary

describe('LogoutResolver', () => {
  let resolver: LogoutResolver;
  let logoutService: LogoutService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        LogoutResolver,
        {
          provide: LogoutService,
          useValue: {
            logout: jest.fn(),
          },
        },
      ],
    }).compile();

    resolver = module.get<LogoutResolver>(LogoutResolver);
    logoutService = module.get<LogoutService>(LogoutService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should call logoutService with the given input', async () => {
    const logoutRequest: LogoutRequestInput = { token: '' }; // token is empty but test is for the call itself
    const expectedResult = 'Successfully logged out';

    jest.spyOn(logoutService, 'logout').mockResolvedValue(expectedResult);

    const result = await resolver.logout(logoutRequest);

    expect(result).toBe(expectedResult);
    expect(logoutService.logout).toHaveBeenCalledWith(logoutRequest);
  });

  it('should call logoutService with valid token', async () => {
    const validToken = 'valid_token';
    const logoutRequest: LogoutRequestInput = { token: validToken };
    const expectedResult = 'Successfully logged out';

    jest.spyOn(logoutService, 'logout').mockResolvedValue(expectedResult);

    const result = await resolver.logout(logoutRequest);

    expect(result).toBe(expectedResult);
    expect(logoutService.logout).toHaveBeenCalledWith(logoutRequest);
  });

  it('should handle errors thrown by logoutService', async () => {
    const validToken = 'valid_token';
    const logoutRequest: LogoutRequestInput = { token: validToken };
    const errorMessage = 'Logout failed';

    jest
      .spyOn(logoutService, 'logout')
      .mockRejectedValue(new Error(errorMessage));

    await expect(resolver.logout(logoutRequest)).rejects.toThrow(errorMessage);
    expect(logoutService.logout).toHaveBeenCalledWith(logoutRequest);
  });
});