import { createCookieSessionStorage } from "@remix-run/node";
import { redirect } from "@remix-run/node";

export interface LoginPayload {
  message: string;
  token: string;
  exp: number;
  user: {
    id: string;
    firstName: string;
    lastName: string;
    golds: number;
    email: string;
    createdAt: string;
    updatedAt: string;
  };
}

const USER_SESSION_KEY = "user";

export const sessionStorage = createCookieSessionStorage({
  cookie: {
    name: "__session",
    httpOnly: true,
    path: "/",
    sameSite: "lax",
    secrets: ["my-secret-cookie-key"],
    secure: process.env.NODE_ENV === "production",
  },
});

async function getSession(request: Request) {
  const cookie = request.headers.get("Cookie");
  return sessionStorage.getSession(cookie);
}

export async function logout(request: Request, urlToRedirect?: string) {
  const session = await getSession(request);
  return redirect(urlToRedirect || "/login", {
    headers: {
      "Set-Cookie": await sessionStorage.destroySession(session),
    },
  });
}

export async function createUserSession({
  request,
  data,
}: {
  request: Request;
  data: LoginPayload;
}) {
  const session = await getSession(request);
  session.set(USER_SESSION_KEY, JSON.stringify(data));

  return redirect("/", {
    headers: {
      "Set-Cookie": await sessionStorage.commitSession(session, {
        maxAge: data.exp,
        expires: new Date(data.exp * 1000),
      }),
    },
  });
}

export async function getUserSession(
  request: Request,
): Promise<LoginPayload | undefined> {
  const session = await getSession(request);
  const user = session.get(USER_SESSION_KEY);
  if (!user) return undefined;
  return JSON.parse(user);
}