import { getConnection, getRepository } from 'typeorm';

import UserSession from '../../../db/entity/userSession';

export default async (refreshToken: string) => {
  const session: UserSession = await getRepository(UserSession)
    .createQueryBuilder('session')
    .where('session.refreshToken = :refreshToken', { refreshToken })
    .leftJoinAndSelect('session.user', 'user')
    .getOne();

  if (session) {
    // eslint-disable-next-line no-return-await
    return await getConnection()
      .createQueryBuilder()
      .delete()
      .from(UserSession)
      .where('refreshToken = :refreshToken', { refreshToken })
      .execute();
  }

  throw new Error('Logout error');
};