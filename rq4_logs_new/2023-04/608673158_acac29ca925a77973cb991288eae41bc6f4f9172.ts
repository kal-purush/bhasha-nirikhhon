import { Body, Controller, Post } from '@nestjs/common';
import {
  ApiBadRequestResponse,
  ApiCreatedResponse,
  ApiOkResponse,
  ApiTags,
  ApiUnauthorizedResponse,
} from '@nestjs/swagger';
import { LoginDTO } from './dtos/login.dto';
import { RegisterDTO } from './dtos/register.dto';
import { TokenDTO } from './dtos/token.dto';

@ApiTags('Auth')
@Controller('auth')
export class AuthController {
  @ApiCreatedResponse({
    description: 'User registered successfully and JWT token generated',
    type: TokenDTO,
  })
  @ApiBadRequestResponse({
    description: 'Invalid request payload or email already registered',
  })
  @Post('register')
  public async register(@Body() registerDto: RegisterDTO): Promise<TokenDTO> {
    return new TokenDTO();
  }

  @Post('login')
  @ApiOkResponse({
    description: 'User logged in successfully and JWT token generated',
    type: TokenDTO,
  })
  @ApiUnauthorizedResponse({
    description: 'Invalid credentials',
  })
  public async login(@Body() registerDto: LoginDTO): Promise<TokenDTO> {
    return new TokenDTO();
  }
}