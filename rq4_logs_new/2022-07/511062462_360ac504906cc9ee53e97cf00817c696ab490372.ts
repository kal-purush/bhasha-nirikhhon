import { Body, Controller, Post } from '@nestjs/common';
import { CreateUserDto } from 'src/user/dto/createUser.dto';
import { LoginUserDto } from 'src/user/dto/loginUser.dto';
import { AuthService } from './auth.service';

@Controller('auth')
export class AuthController {
  constructor(private authService: AuthService) {}

  @Post('/registration')
  async registration(
    @Body() userDto: CreateUserDto,
    // @Res({ passthrough: true }) res: Response,
  ) {
    const userData = await this.authService.registration(userDto);
    // res.cookie('refreshToken', userData.refreshToken, {
    //   maxAge: 30 * 24 * 60 * 100,
    //   httpOnly: true,
    // });
    return userData;
  }

  @Post('/login')
  async login(
    @Body() userDto: LoginUserDto,
    // @Res({ passthrough: true }) res: Response,
  ) {
    const userData = await this.authService.login(userDto);
    // res.cookie('refreshToken', userData.refreshToken, {
    //   maxAge: 30 * 24 * 60 * 100,
    //   httpOnly: true,
    // });
    return userData;
  }

  // @Post('/logout')
  // async logout(@Req() req: Request, @Res() res: Response) {
  //   const { refreshToken } = req.cookies;
  //   //@ts-ignore
  //   const { email } = jwt.verify(refreshToken, process.env.JWT_REFRESH_SECRET);
  //   res.clearCookie('refreshToken');
  //   return this.userService.logout(email);
  // }
}