import type { DeletedObjectJSON } from '@clerk/nextjs/server';
import { log } from 'next-axiom';
import { db } from '~/server/db';

export async function processUserDeleted(user: DeletedObjectJSON): Promise<void> {
  log.debug('user deleted from db');
  const userExists = await db.user.findUnique({ where: { externalId: user.id } });
  if (!userExists) return;

  const deleted = await db.user.delete({ where: { externalId: user.id } });
  log.debug('deleted user', { deleted });
}