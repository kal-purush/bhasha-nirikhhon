import { Body, Controller, Post, UseGuards, ValidationPipe } from '@nestjs/common';
import { AuthGuard } from '@nestjs/passport';
import { AuthService } from './AuthService';
import { AuthCredentialDto } from './dto/AuthCredentialDto';
import { GetUser } from './GetUserDecorator';
import { User } from './User.entity';

@Controller('auth')
export class AuthController {
  constructor(
    private authService: AuthService
  ) {}

  @Post('/register')
  register(@Body(new ValidationPipe({ transform: true })) authCredentialDto: AuthCredentialDto): Promise<void> {
    return this.authService.register(authCredentialDto);
  }

  @Post('/signin')
  signIn(@Body(new ValidationPipe({ transform: true })) authCredentialDto: AuthCredentialDto): Promise<{accessToken: string}> {
    return this.authService.signIn(authCredentialDto);
  }

  @Post('/test')
  @UseGuards(AuthGuard())
  test(@GetUser() user: User) {
      console.log(user);
  }
}