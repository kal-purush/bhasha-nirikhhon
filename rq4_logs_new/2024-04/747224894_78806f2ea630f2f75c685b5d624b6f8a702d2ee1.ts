import { clerkClient as clerk } from '@clerk/nextjs';
import { db } from '~/server/db';
import { log } from 'next-axiom';
import type { User } from '@prisma/client';

export async function createUserInDatabase(userId: string): Promise<User> {
  const user = await clerk.users.getUser(userId);
  const email = user.emailAddresses[0]?.emailAddress;
  const emailParts = email?.split('@') ?? [];
  emailParts.pop();
  const emailLocalPart = emailParts.join('@').toLowerCase();

  const existingUser = await db.user.findUnique({ where: { email } });
  if (existingUser) {
    log.debug('user already exists in db');
    return existingUser;
  }

  log.debug('creating user in db');
  const userInDb = await db.user.create({
    data: {
      username: user.username,
      externalId: user.id,
      firstName: user.firstName,
      lastName: user.lastName,
      imageUrl: user.imageUrl,
      email,
      emailLocalPart,
    },
  });

  log.debug('updating user in clerk');
  const userUpdate = await clerk.users.updateUser(user.id, { externalId: userInDb.id });
  log.debug('updated user in clerk', { userUpdate });

  return userInDb;
}