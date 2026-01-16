import { type NextRequest, NextResponse } from 'next/server';

import { ACCESS_TOKEN } from './constants';

// TODO: add private pages
const privatePgaes = ['/mypage'];

export async function middleware(request: NextRequest) {
  const response = NextResponse.next();
  const { pathname } = request.nextUrl;
  const accessToken = request.cookies.get(ACCESS_TOKEN);

  if (privatePgaes.includes(pathname) && !accessToken) {
    return NextResponse.redirect(new URL('/login', request.url));
  }
  if (pathname === '/login' && accessToken) {
    return NextResponse.redirect(new URL('/', request.url));
  }

  return response;
}

export const config = {
  matcher: ['/:path*'],
};