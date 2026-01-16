import { routes } from "@/constants/routes";
import { NextRequest, NextResponse } from "next/server";

export async function middleware(req: NextRequest) {
  const { pathname } = req.nextUrl;

  const authCookie = req.cookies.get("role")?.value;

  // Check for the dashboard route
  if (pathname.startsWith(`${routes.dashboard}`)) {
    if (authCookie !== "ADMIN") {
      return NextResponse.redirect(new URL(`${routes.signIn}`, req.url));
    }
  }

  // Check for the checkout route
  else if (pathname.startsWith(`${routes.checkout}`)) {
    if (authCookie !== "USER") {
      return NextResponse.redirect(new URL(`${routes.signIn}`, req.url));
    }
  }

  // Check for the user profile route
  else if (pathname.startsWith(`${routes.userProfile}`)) {
    if (authCookie !== "USER") {
      return NextResponse.redirect(new URL(`${routes.signIn}`, req.url));
    }
  }

  return NextResponse.next();
}

export const config = {
  matcher: [
    `${routes.dashboard}/:path*`,
    `${routes.checkout}/:path*`,
    `${routes.userProfile}/:path*`,
  ],
};