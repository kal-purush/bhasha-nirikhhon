import { Prisma } from '@prisma/client';
import { prisma } from '..';
import { tokenLifeTime } from '@utils/utils/default/token-lifetime';

export function ResetTokenModel(tx?: Prisma.TransactionClient) {
  const db = tx || prisma;

  return {
    resetToken: db.resetToken,

    async create(data: { userId: string }): Promise<{ token: string }> {
      return await db.resetToken.create({
        data: {
          token: Math.random().toString(36).substring(2),
          userId: data.userId,
          expiresAt: tokenLifeTime,
        },
      });
    },

    async findToken(
      token: string,
    ): Promise<{ userId: string; id: string } | null> {
      return await db.resetToken.findFirst({
        where: {
          token,
        },
      });
    },

    async deleteToken(token: string): Promise<{ userId: string } | null> {
      return await db.resetToken.delete({
        where: {
          token,
        },
      });
    },
  };
}