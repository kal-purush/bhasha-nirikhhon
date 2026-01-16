// middleware.ts
import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

export function middleware(request: NextRequest) {
  const url = request.nextUrl.clone();
  const { pathname } = url;
  
  // 인증 쿠키 확인 및 디버깅 로그
  const authToken = request.cookies.get('auth_token')?.value;
  const isAuthenticated = !!authToken;
  
  // 디버깅 정보를 응답 헤더에 추가 (개발 환경에서만)
  const response = NextResponse.next();
  response.headers.set('x-middleware-cache', 'no-cache');
  response.headers.set('x-debug-path', pathname);
  response.headers.set('x-debug-auth', isAuthenticated ? 'true' : 'false');
  

  // 로그인 화면 경로
  const loginPath = "/";

  // 정적 자원 및 API 경로 허용
  if (
    pathname.startsWith("/_next/") ||
    pathname.startsWith("/api/") ||
    pathname.match(/\.(jpg|jpeg|png|gif|svg|ico|css|js)$/)
  ) {
    return NextResponse.next();
  }

  // 공개 경로 패턴
  const publicPathPatterns = [
    /^\/$/, // 정확히 홈페이지만
  ];

  // 허용된 경로 패턴 (로그인 필요)
  const allowedPathPatterns = [
    /^\/pos(\/.*)?$/, // pos와 그 하위 경로
    /^\/create(\/.*)?$/, // create와 그 하위 경로
    /^\/setting(\/.*)?$/,
    /^\/editcategory(\/.*)?$/,
    /^\/editmenu(\/.*)?$/,
    /^\/home(\/.*)?$/,
    /^\/payment(\/.*)?$/,
  ];

  // 공개 경로 확인 (정규식 패턴 사용)
  const isPublicPath = publicPathPatterns.some((pattern) =>
    pattern.test(pathname)
  );

  if (isPublicPath) {
    return NextResponse.next();
  }

  // 로그인하지 않은 경우 로그인 페이지로 리디렉션
  if (!isAuthenticated) {
    url.pathname = loginPath;
    url.searchParams.set("callbackUrl", pathname);
    return NextResponse.redirect(url);
  }

  // 로그인한 경우 허용된 경로인지 확인
  const isAllowedPath = allowedPathPatterns.some((pattern) =>
    pattern.test(pathname)
  );

  // 로그인한 사용자가 허용되지 않은 경로로 접근할 때
  if (!isAllowedPath) {
    // 허용되지 않은 경로는 home으로 리디렉션 (pos 대신)
    url.pathname = "/home";
    return NextResponse.redirect(url);
  }

  return NextResponse.next();
}

// 미들웨어 적용 경로 설정
export const config = {
  matcher: [
    // 특정 패턴의 경로에만 미들웨어 적용
    "/((?!_next/static|_next/image|favicon.ico).*)",
    // 동적 경로 패턴 예시 (필요시)
    "/orders/:path*",
    "/products/:id/edit",
  ],
};