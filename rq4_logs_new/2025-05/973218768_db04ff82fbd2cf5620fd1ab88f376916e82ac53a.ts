import { Body, Controller, Post } from '@nestjs/common';
import { AuthService } from './services/auth.service';
import { CreateUserDto } from '../users/dto/create-user.dto';
import { SignInDto } from './dto/sign-in.dto';
import { RefreshTokenDto } from './dto/tokens.dto';

@Controller('auth')
export class AuthController {
  constructor(private readonly authService: AuthService) {}

  @Post('register')
  register(@Body() userBody: CreateUserDto) {
    return this.authService.registerUser(userBody);
  }

  @Post('login')
  signIn(@Body() userCredential: SignInDto) {
    return this.authService.signIn(userCredential);
  }

  @Post('refresh-token')
  refreshToken(@Body() { token }: RefreshTokenDto) {
    return this.authService.refreshToken(token);
  }
}