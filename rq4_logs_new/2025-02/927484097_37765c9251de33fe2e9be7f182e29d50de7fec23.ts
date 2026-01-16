import type { Password, User } from "@prisma/client";
import { prisma } from "./db";
import bcrypt from "bcryptjs";

interface AuthCredentials {
  email: User["email"];
  password: Password["hash"];
}

// Cookie Expiration Time
const SESSION_EXPIRATION_TIME = 1000 * 60 * 60 * 24 * 30; // 30 days

export function getSessionExpirationDate() {
  const expirationDate = new Date(Date.now() + SESSION_EXPIRATION_TIME);
  return expirationDate;
}

export async function verifyUserPassword({ email, password }: AuthCredentials) {
  const userWithPassword = await prisma.user.findUnique({
    where: {
      email,
    },
    select: { id: true, password: { select: { hash: true } }, username: true },
  });

  if (!userWithPassword || !userWithPassword.password) {
    return null;
  }

  const isValid = await bcrypt.compare(
    password,
    userWithPassword.password.hash,
  );

  if (!isValid) {
    return null;
  }

  return { id: userWithPassword.id, username: userWithPassword.username }; // Makes sense to get username for redirect here...
}

export async function login({ email, password }: AuthCredentials) {
  const user = await verifyUserPassword({ email, password });

  if (!user) return null;

  const session = await prisma.session.create({
    data: {
      userId: user.id,
      expirationDate: getSessionExpirationDate(),
    },
    select: { id: true, expirationDate: true, userId: true },
  });

  return { session, user };
}