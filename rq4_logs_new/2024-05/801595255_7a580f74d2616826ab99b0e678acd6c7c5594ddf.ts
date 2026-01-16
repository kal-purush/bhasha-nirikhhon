import prisma from "@/lib/db";
import { User } from "@prisma/client";

const getUserByEmail = async (email: string): Promise<User | null> => {
  try {
    const user = await prisma.user.findUnique({
      where: {
        email
      }
    });

    return user
  } catch {
    return null;
  }
}

const getUserById = async (id: string): Promise<User | null> => {
  try {
    const user = await prisma.user.findUnique({
      where: {
        id
      }
    });

    return user
  } catch {
    return null;
  }
}

export { getUserByEmail }