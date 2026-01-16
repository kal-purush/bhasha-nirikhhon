import { prisma } from "@/lib/prisma";

export interface IDbResetPasswordToken {
  id: string;
  token: string;
  userId: string;
  expiresAt: Date;
  createdAt: Date;
}

export interface IResetPasswordTokenObj {
  token: string;
  userId: string;
  expiresAt: Date;
}

export interface IResetPasswordRepository {
  createResetPasswordToken(tokenObj: IResetPasswordTokenObj): Promise<void>;
  findResetPasswordToken(userId: string): Promise<IDbResetPasswordToken | null>;
  deleteAllResetPasswordTokens(userId: string): Promise<void>;
}

export default class ResetPasswordRepository implements IResetPasswordRepository {
  async createResetPasswordToken(tokenObj: IResetPasswordTokenObj): Promise<void> {
    await prisma.resetPasswordToken.create({
      data: tokenObj,
    });
  }

  async findResetPasswordToken(userId: string): Promise<IDbResetPasswordToken | null> {
    return await prisma.resetPasswordToken.findFirst({
      where: { userId },
    });
  }

  async deleteAllResetPasswordTokens(userId: string): Promise<void> {
    await prisma.resetPasswordToken.deleteMany({
      where: { userId },
    });
  }
}