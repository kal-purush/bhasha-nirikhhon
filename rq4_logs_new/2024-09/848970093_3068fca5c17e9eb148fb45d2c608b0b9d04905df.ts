import { Test, TestingModule } from '@nestjs/testing';
import { AuthController } from '../auth.controller';
import { SignInService } from '../services/signin.service';

describe('AdminController', () => {
  let controller: AuthController;

  let signIn: SignInService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [AuthController],
      providers: [
        {
          provide: SignInService,
          useValue: {
            validateUser: jest.fn().mockResolvedValue(null),
            execute: jest.fn().mockResolvedValue(null),
          },
        },
      ],
    }).compile();

    controller = module.get<AuthController>(AuthController);
    signIn = module.get<SignInService>(SignInService);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});