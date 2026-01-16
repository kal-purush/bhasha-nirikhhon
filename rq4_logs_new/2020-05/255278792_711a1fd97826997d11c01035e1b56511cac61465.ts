import bcrypt from 'bcrypt';
import * as jwt from 'jsonwebtoken';
import { Body, HttpError, JsonController, Post } from 'routing-controllers';
import { User, UserAuth } from '../entity/User';
import { BaseController } from './BaseController';

@JsonController()
export class AuthController extends BaseController {
  @Post('/login')
  public async authenticationUser(@Body({ validate: true }) body: UserAuth) {
    const user: User | undefined = await User.findOne({
      where: {
        email: body.email,
      },
    });
    if (!user) throw new HttpError(404, '해당 사용자를 찾을 수 없습니다.');

    const result = await bcrypt.compare(body.password, user.password);
    if (!result) throw new HttpError(404, '비밀번호가 일치하지 않습니다.');

    const payload = {
      email: user.email,
    };
    const options = { expiresIn: '1h' };
    const token = await jwt.sign(payload, process.env.JWT_SECRET_KEY!, options); // process.env.JWT_SECRET_KEY -> ! 안 붙여주면 에러
    if (!token) throw new HttpError(500, '토큰 발급 실패 !');
    return { token: token };
  }
}