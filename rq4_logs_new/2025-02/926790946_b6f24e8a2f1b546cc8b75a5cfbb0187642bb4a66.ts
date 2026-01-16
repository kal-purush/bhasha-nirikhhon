import { prisma } from "@/lib/prisma";

export interface IDbUser {
  name: string;
  email: string;
  password: string;
}

export interface IUserRepository {
  createUser(newUserObj: IDbUser): Promise<void>;
  findUserByEmail(email: string): Promise<IDbUser | null>;
  findUserById(userId: string): Promise<IDbUser | null>;
  updatePassword(userId: string, newPassword: string): Promise<void>;
}

export default class UserRepository implements IUserRepository {
  async createUser(newUserObj: IDbUser): Promise<void> {
    await prisma.user.create({
      data: newUserObj,
    });
  }

  async findUserByEmail(email: string): Promise<IDbUser | null> {
    return await prisma.user.findUnique({
      where: { email },
    });
  }

  async findUserById(userId: string): Promise<IDbUser | null> {
    return await prisma.user.findUnique({
      where: { id: userId },
    });
  }

  async updatePassword(userId: string, newPassword: string): Promise<void> {
    await prisma.user.update({
      where: { id: userId },
      data: { password: newPassword },
    });
  }
}