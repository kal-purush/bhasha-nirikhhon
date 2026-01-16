import { createCookieSessionStorage, redirect } from "@remix-run/node";
import invariant from "tiny-invariant";

invariant(process.env.SESSION_SECRET, "SESSION_SECRET mut be set");

export const sessionStorage = createCookieSessionStorage({
  cookie: {
    name: "__session",
    httpOnly: true,
    path: "/",
    sameSite: "lax",
    secrets: [process.env.SESSION_SECRET],
    secure: true,
  },
});

export async function getSession(request: Request) {
  const cookie = request.headers.get("Cookie");
  return sessionStorage.getSession(cookie);
}

export async function createUserSession({
  request,
  user,
  userId,
  remember,
  token,
  redirectTo,
}: {
  request: Request;
  user: any;
  userId: string;
  remember: boolean;
  token: string;
  redirectTo: string;
}) {
  const session = await getSession(request);
  session.set("userId", userId);
  session.set("user", user);
  session.set("token", token);
  return redirect(redirectTo, {
    headers: {
      "Set-Cookie": await sessionStorage.commitSession(session, {
        maxAge: 60 * 60 * 2,
      }),
    },
  });
}

export async function logout(request: Request) {
  const session = await getSession(request);
  return redirect("/", {
    headers: {
      "Set-Cookie": await sessionStorage.destroySession(session),
    },
  });
}

export async function getUser(request: Request) {
  const session = await getSession(request);
  const user = session.get("user");

  if (user === undefined) {
    return null;
  }
  if (user) {
    return user;
  }

  throw await logout(request);
}