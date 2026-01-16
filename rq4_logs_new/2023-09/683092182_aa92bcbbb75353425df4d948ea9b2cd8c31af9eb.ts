import { NextRequest, NextResponse, NextFetchEvent } from 'next/server';

// Limit the middleware to paths in matcher array.
export const config = {
    matcher: ['/mypage/:path*', '/interview/:path*', '/category/:path*'],
};

export function middleware(request: NextRequest) {
    // TODO - authenticated 판별 수정 -> cookie
    if (false) {
        return NextResponse.redirect(new URL('/auth/login', request.url));
    }
}