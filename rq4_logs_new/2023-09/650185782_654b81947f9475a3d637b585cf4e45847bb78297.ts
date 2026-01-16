import { AuthService } from './auth.service';
import { UsersService } from '../users/users.service';
import { CreateUserDto } from 'src/users/dto/create-user.dto';
import { SignInDto } from 'src/users/dto/sign_in.dto';
export declare class AuthController {
    private readonly authService;
    private readonly usersService;
    constructor(authService: AuthService, usersService: UsersService);
    signUp(registerUserDto: CreateUserDto): Promise<import("../users/user.entity").User>;
    signIn(signInDto: SignInDto): Promise<{
        access_token: string;
    }>;
}