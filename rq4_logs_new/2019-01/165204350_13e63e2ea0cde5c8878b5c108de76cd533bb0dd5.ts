import { Body, Controller, HttpStatus, Post, Response } from '@nestjs/common';
import { User } from '../user/entity/user.entity';
import { UserService } from '../user/user.service';
import { AuthService } from './auth.service';

@Controller('auth')
export class AuthController {
  constructor(
    private readonly authService: AuthService,
    private readonly userService: UserService,
  ) {}

  @Post('login')
  async createToken(@Body() payload): Promise<any> {
    return await this.authService.createToken(payload.email);
  }

  // @Post('login')
  // async loginUser(@Response() res: any, @Body() body: User) {
  //   if (!(body && body.email && body.password)) {
  //     return res
  //       .status(HttpStatus.FORBIDDEN)
  //       .json({ message: 'Username and password are required!' });
  //   }
  //
  //   const user = await this.userService.findOneByEmail(body.email);
  //
  //   if (user.length > 0) {
  //     if (await this.userService.compareHash(body.password, user[0].password)) {
  //       return res
  //         .status(HttpStatus.OK)
  //         .json(await this.authService.createToken(user[0].email));
  //     }
  //   }
  //
  //   return res
  //     .status(HttpStatus.FORBIDDEN)
  //     .json({ message: 'Username or password wrong!' });
  // }

  // @Post('register')
  // async registerUser(@Response() res: any, @Body() body: User) {
  //   if (!(body && body.email && body.password)) {
  //     return res
  //       .status(HttpStatus.FORBIDDEN)
  //       .json({ message: 'Username and password are required!' });
  //   }
  //
  //   let user = await this.userService.findOneByEmail(body.email)[0];
  //
  //   if (user) {
  //     return res
  //       .status(HttpStatus.FORBIDDEN)
  //       .json({ message: 'Username exists' });
  //   } else {
  //     user = await this.userService.create(body);
  //     if (user) {
  //       user.passwordHash = undefined;
  //     }
  //   }
  //
  //   return res.status(HttpStatus.OK).json(user);
  // }
}