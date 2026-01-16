import { Test, TestingModule } from '@nestjs/testing';
import { JwtService } from '@nestjs/jwt';
import { AuthResolver } from './auth.resolver';
import { UserService } from '../user/user.service';
import * as bcrypt from 'bcryptjs';
import { User } from '../user/models/user.entity';

describe('AuthResolver', () => {
  let resolver: AuthResolver;
  let userService: UserService;
  let jwtService: JwtService;

  beforeEach(async () => {
    // Mock UserService and JwtService
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        AuthResolver,
        {
          provide: UserService,
          useValue: {
            findByEmail: jest.fn(),
            create: jest.fn(),
          },
        },
        {
          provide: JwtService,
          useValue: {
            sign: jest.fn(),
          },
        },
      ],
    }).compile();

    resolver = module.get<AuthResolver>(AuthResolver);
    userService = module.get<UserService>(UserService);
    jwtService = module.get<JwtService>(JwtService);
  });

  it('should be defined', () => {
    expect(resolver).toBeDefined();
  });

  describe('login', () => {
    it('should return error if user does not exist', async () => {
      jest.spyOn(userService, 'findByEmail').mockResolvedValueOnce(null);
      expect(await resolver.login('test@example.com', 'password')).toEqual({
        code: 10002,
        message: "account doesn't exist",
      });
    });

    it('should return error if password is invalid', async () => {
      jest.spyOn(userService, 'findByEmail').mockResolvedValueOnce({
        id: 'a9868b30-51bd-4070-8dbb-043a56e21bcb',
        email: 'wethanw.001@gmail.com',
        password: 'YourSecurePassword', 
      } as User);

      jest.spyOn(bcrypt, 'compare' as any).mockResolvedValueOnce(false);

      expect(await resolver.login('test@example.com', 'wrongpassword')).toEqual({
        code: 10003,
        message: 'login failed, wrong password',
      });
    });

    it('should return token if login is successful', async () => {
      jest.spyOn(userService, 'findByEmail').mockResolvedValueOnce({ 
        id: 'a9868b30-51bd-4070-8dbb-043a56e21bcb',
        email: 'wethanw.001@gmail.com',
        password: 'YourSecurePassword', 
    } as User);
      jest.spyOn(bcrypt, 'compare' as any).mockResolvedValueOnce(true);
      jest.spyOn(jwtService, 'sign').mockReturnValue('token');

      expect(await resolver.login('test@example.com', 'password')).toEqual({
        code: 200,
        message: 'login successful',
        data: 'token',
      });
    });
  });
});