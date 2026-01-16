import { db } from "~/utils/prisma.server";
import { createCookieSessionStorage } from "@remix-run/node";
import { compare } from "bcryptjs";

const sessionSecret = process.env.SESSION_SECRET;
if (!sessionSecret) throw Error("Please set a session secret");

const { commitSession, destroySession, getSession } = createCookieSessionStorage({
  cookie: {
    name: "auth_session",
    secrets: [sessionSecret], //TODO: Set secrets in netlify
    sameSite: "lax",
    secure: true,
    httpOnly: true
  }
});

type LoginData = {
  username: string;
  password: string;
};

export const login = async ({ username, password }: LoginData) => {
  const user = await db.user.findUnique({ where: { username } });
  if (!user) return null;

  const isPasswordMatched = await compare(password, user.passwordHash);
  if (!isPasswordMatched) return null;

  return { id: user.id, username };
};