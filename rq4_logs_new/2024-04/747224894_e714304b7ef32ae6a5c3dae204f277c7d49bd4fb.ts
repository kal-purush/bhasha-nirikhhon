import { clerkClient as clerk } from '@clerk/nextjs';
import type { UserJSON } from '@clerk/nextjs/server';
import { log } from 'next-axiom';
import { db } from '~/server/db';

export async function processUserCreated(user: UserJSON): Promise<void> {
  const email = user.email_addresses[0]?.email_address;
  const emailParts = email?.split('@') ?? [];
  emailParts.pop();
  const emailLocalPart = emailParts.join('@').toLowerCase();

  const userData = {
    username: user.username,
    externalId: user.id,
    firstName: user.first_name,
    lastName: user.last_name,
    imageUrl: user.image_url,
    email,
    emailLocalPart,
  };

  if (await db.user.findUnique({ where: { externalId: user.id } })) {
    return;
  }

  const userInDb = await db.user.create({
    data: userData,
  });

  await clerk.users.updateUser(user.id, { externalId: userInDb.id });
  log.debug('updated user in clerk');
}