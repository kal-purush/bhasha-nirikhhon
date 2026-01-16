import { CACHE_MANAGER, CacheModule } from '@nestjs/common';
import { ConfigModule, ConfigType } from '@nestjs/config';
import { TokenService } from '../../../../src/domain/auth/service/token.service';
import { Test } from '@nestjs/testing';
import { HttpModule, HttpService } from '@nestjs/axios';
import { JwtModule } from '@nestjs/jwt';
import { Cache } from 'cache-manager';
import { SnsService } from '../../../../src/domain/auth/service/sns.service';
import { PrismaModule } from '../../../../src/global/prisma/prisma.module';
import oauthConfig from '../../../../src/global/configs/oauth.config';
import { UserRepository } from '../../../../src/domain/user/repository/user.repository';
import jwtConfig from '../../../../src/global/configs/jwt.config';

const mockConfigType = () => ({
  kakaoClientId: 'kakao',
  kakaoAdminKey: 'admin',
});
const mockKakaoPublicKey = () => ({
  data: {
    keys: [
      {
        kid: 'kid1',
      },
      { kid: 'kid2' },
    ],
  },
});

describe('sns provider unit test', () => {
  let httpService: HttpService;
  let cacheManager: Cache;
  let tokenService: TokenService;
  let snsService: SnsService;
  let config: ConfigType<typeof oauthConfig>;

  beforeEach(async () => {
    const app = await Test.createTestingModule({
      imports: [
        PrismaModule,
        HttpModule,
        JwtModule.register({}),
        CacheModule.register(),
      ],
      providers: [
        SnsService,
        UserRepository,
        {
          provide: CACHE_MANAGER,
          useValue: {
            get: jest.fn(),
            set: jest.fn(),
          },
        },
        {
          provide: TokenService,
          useValue: {
            getKidFromIdToken: jest.fn().mockImplementation(() => 'kid1'),
            idTokenVerify: jest.fn().mockImplementation(() => {
              return {
                sub: '1',
              };
            }),
          },
        },
        {
          provide: oauthConfig.KEY,
          useValue: {
            ...mockConfigType,
          },
        },
      ],
    }).compile();

    httpService = app.get<HttpService>(HttpService);
    cacheManager = app.get<Cache>(CACHE_MANAGER);
    tokenService = app.get<TokenService>(TokenService);
    config = app.get<ConfigType<typeof oauthConfig>>(oauthConfig.KEY);
    snsService = app.get<SnsService>(SnsService);
  });

  describe('카카오idToken 검증 요청 ', () => {
    it('test', async () => {
      //given
      const KAKAO_PUBLIC_KEY_CAHCE = 'cacheKey';
      const CollectKid = 'kid1';
      const idtoken = 'idtoken';
      const getCacehMock = jest
        .spyOn(cacheManager, 'get')
        .mockResolvedValue(undefined);
      // const mockKey = jest
      //   .spyOn(tokenService, 'getKidFromIdToken')
      //   .mockResolvedValue(CollectKid);
      const httpMock = jest
        .spyOn(httpService.axiosRef, 'get')
        .mockResolvedValue(mockKakaoPublicKey());
      const getCacehMock2 = jest
        .spyOn(cacheManager, 'get')
        .mockResolvedValue(mockKakaoPublicKey());

      await snsService.kakaoIdTokenVerify(idtoken);

      expect(httpMock).toBeCalledTimes(1);
    });
  });
});