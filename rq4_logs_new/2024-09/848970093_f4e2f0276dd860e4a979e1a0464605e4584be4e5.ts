import { Test, TestingModule } from '@nestjs/testing';
import { SignInService } from '../services/signin.service';
import { JwtService } from '@nestjs/jwt';
import { SecurityService } from '../../../common/services/security.service';
import { PrismaService } from '../../../prisma.service';

describe('AuthService', () => {
  let signInService: SignInService;

  let jwtService: JwtService;
  let securityService: SecurityService;
  let prisma: PrismaService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [SignInService, JwtService, PrismaService, SecurityService],
    }).compile();

    signInService = module.get<SignInService>(SignInService);

    jwtService = module.get<JwtService>(JwtService);
    securityService = module.get<SecurityService>(SecurityService);
    prisma = module.get<PrismaService>(PrismaService);
  });

  it('should be defined', () => {
    expect(signInService).toBeDefined();
    expect(jwtService).toBeDefined();
    expect(securityService).toBeDefined();
    expect(prisma).toBeDefined();
  });
});