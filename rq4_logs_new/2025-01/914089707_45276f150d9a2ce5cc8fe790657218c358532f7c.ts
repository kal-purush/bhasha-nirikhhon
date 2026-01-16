import { Controller, Post, Body, ValidationPipe } from '@nestjs/common';
import { AuthService } from './auth.service';
import { AuthCredentialDto } from './dto/auth-credential.dto';

// define the response type
export interface UserResponse {
  status: number;
  message: string;
  data?: any;
}

@Controller('auth')
export class AuthController {
  constructor(private readonly authService: AuthService) {}

  @Post('sign-up')
  async signup(
    @Body(ValidationPipe) authCredentialDto: AuthCredentialDto,
  ): Promise<UserResponse> {
    return this.authService.signup(authCredentialDto);
  }
}