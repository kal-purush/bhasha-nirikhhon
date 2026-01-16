import { withAuth } from "next-auth/middleware";
import { NextResponse } from "next/server";

// middleware is applied to all routes, use conditionals to select

let protectedRoutes = [
  {
    path: /\/admin\/users(\/|)[A-Za-z]?/i,
    roles: ["SuperAdmin"],
  },
];

export default withAuth(
  function middleware(req) {
    const { token } = req.nextauth;
    const { pathname } = req.nextUrl;

    if (!token) return NextResponse.redirect("/auth/signin");
    if (token.role === "Guest") {
      return NextResponse.rewrite(new URL("/unauthorized", req.url));
    }

    const route = protectedRoutes.find((route) => route.path.test(pathname));

    if (route && !route.roles.includes(token.role)) {
      return NextResponse.rewrite(new URL("/unauthorized", req.url));
    }
  },
  {
    pages: {
      signIn: "/auth/signin",
    },
  },
);

export const config = {
  matcher: ["/admin/:path*", "/admin"],
};