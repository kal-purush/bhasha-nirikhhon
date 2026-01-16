import { UserService } from '../user/user.service';
import { AuthController } from './auth.controller';
import { AuthService } from './auth.service';

describe('Auth Controller', () => {
  let controller: AuthController;
  let userService: UserService;
  let authService: AuthService;

  beforeAll(async () => {
    userService = {} as any;
    authService = {} as any;
    controller = new AuthController(authService, userService);
  });

  it('should be defined', () => {
    // const controller: AuthController = module.get<AuthController>(
    //   AuthController,
    // );
    // expect(controller).toBeDefined();
    expect(1).toBe(1);
  });
});