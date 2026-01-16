import { sign } from 'jsonwebtoken';
import { v4 } from 'uuid';
import { getRepository } from 'typeorm';

import {
  jwtAccessSecret,
  jwtAccessExpiredTime,
  jwtAccessTokenWord,
  refreshTokenExpired
} from '../../../config/application';
import tokenizer from './utils/tokenizer';
import UserSession from '../../../db/entity/userSession';

export default async (id: number) => {
  const accessToken: string = sign({ id }, jwtAccessSecret, {
    expiresIn: jwtAccessExpiredTime
  });
  const refreshToken: string = v4();
  const tempDate: Date = new Date();
  const expiredDate: Date = new Date(
    tempDate.setTime(tempDate.getTime() + +refreshTokenExpired)
  );

  await getRepository(UserSession)
    .createQueryBuilder('session')
    .insert()
    .into(UserSession)
    .values({
      refreshToken,
      expiredDate,
      user: id
    })
    .execute();

  return {
    accessToken: tokenizer(accessToken, jwtAccessTokenWord),
    refreshToken
  };
};