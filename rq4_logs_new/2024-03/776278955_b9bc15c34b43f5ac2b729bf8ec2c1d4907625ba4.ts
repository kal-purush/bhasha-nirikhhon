import { NextResponse, type NextRequest } from "next/server";

export function middleware(request: NextRequest) {
  if (request.nextUrl.pathname === "/") {
    // http://localhost:3000/?query=1 の時
    const query = request.nextUrl.searchParams.get("query");
    console.log(query); // 1
    console.log(request.nextUrl.search); // ?query=1
  }

  if (request.nextUrl.pathname.startsWith("/test-redirect/hoge")) {
    // リダイレクト（どのパターンでもできる）
    // 【パターン1】
    request.nextUrl.pathname = "/";
    return NextResponse.redirect(request.nextUrl);
    // 【パターン2】
    // return NextResponse.redirect(new URL("/", request.url));
    // 【パターン3】URL変わらないでページ内容がTOPになる
    // return NextResponse.rewrite(new URL("/", request.url));
  }

  const response = NextResponse.next();
  return response;
}

export const config = {
  // 適用するパス制限できる
  // matcher: "/about/:path*",
};