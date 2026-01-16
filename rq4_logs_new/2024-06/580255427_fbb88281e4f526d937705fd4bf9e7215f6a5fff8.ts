import { Test, TestingModule } from '@nestjs/testing';
import { AuthTokenService } from './auth-token.service';
import { CACHE_MANAGER } from '@nestjs/cache-manager';
import { JwtService } from '@nestjs/jwt';

describe('AuthTokenService', () => {
  let service: AuthTokenService;

  const mockCacheManager = {
    get: jest.fn(),
    set: jest.fn(),
  };

  const mockJwtService = {
    signAsync: jest.fn(),
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        AuthTokenService,
        {
          provide: CACHE_MANAGER,
          useValue: mockCacheManager,
        },
        {
          provide: JwtService,
          useValue: mockJwtService,
        },
      ],
    }).compile();

    service = module.get<AuthTokenService>(AuthTokenService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});