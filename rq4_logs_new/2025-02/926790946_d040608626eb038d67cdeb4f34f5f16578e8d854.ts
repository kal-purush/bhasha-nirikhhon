import { prisma } from "@/lib/prisma";

interface ISessionToken {
  id: string;
  token: string;
  userId: string;
  expiresAt: Date;
  createdAt: Date;
}

export interface ITokenObj {
  token: string;
  userId: string;
  expiresAt: Date;
}

export interface IAuthRepository {
  createSessionToken(tokenObj: ITokenObj): Promise<void>;
  findToken(token: string): Promise<ISessionToken | null>;
  deleteAllTokens(userId: string): Promise<void>;
}

export default class AuthRepository implements IAuthRepository {
  async createSessionToken(tokenObj: ITokenObj): Promise<void> {
    await prisma.sessionToken.create({
      data: tokenObj,
    });
  }

  async findToken(token: string): Promise<ISessionToken | null> {
    return await prisma.sessionToken.findUnique({
      where: { token },
    });
  }

  async deleteAllTokens(userId: string): Promise<void> {
    await prisma.sessionToken.deleteMany({
      where: { userId },
    });
  }
}