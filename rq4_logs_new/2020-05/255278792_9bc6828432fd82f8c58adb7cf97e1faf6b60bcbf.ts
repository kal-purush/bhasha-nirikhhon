import { NextFunction, Request, Response } from 'express';
import * as jwt from 'jsonwebtoken';
import { ExpressMiddlewareInterface, UnauthorizedError } from 'routing-controllers';

export class JwtAuth implements ExpressMiddlewareInterface {
  use(req: Request, res: Response, next: NextFunction) {
    const token: any = req.headers['x-access-token'];
    if (!token) return res.json(new UnauthorizedError('로그인을 위한 토큰이 없습니다.'));

    jwt.verify(token, process.env.JWT_SECRET_KEY!, (err: any, info: any) => {
      if (err) return res.json(new UnauthorizedError('인증에 실패했습니다.'));
      console.log(info);
      // 여기서 인증만 되면 해당 토큰으로 다른 서비스를 이용할 수 있는 건지 ?!
      // 아니면 토큰에 이메일을 담았는데 라우터에서 토큰의 이메일과 id로 조회한 이메일이 같음을 검증해야 하는 건지?!
      // req.user = info;
      return next();
    });
  }
}