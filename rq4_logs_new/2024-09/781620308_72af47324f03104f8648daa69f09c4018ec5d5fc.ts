import { RateLimiter } from "limiter";
const limiter = new RateLimiter({
  tokensPerInterval: 10,
  interval: "minute",
  fireImmediately: true
});

import { NextResponse } from 'next/server'
import type { NextRequest } from 'next/server'
 
export async function middleware(request: NextRequest) {
    const remainingRequests = await limiter.removeTokens(1);
    console.log(remainingRequests)
  if (remainingRequests < 0) {
    return Response.json(
      { success: false, message: 'authentication failed' },
      { status: 401 }
    )
  } 
  return NextResponse.next();
} 

export const config = {
    matcher:[
        '/((?!api|_next/static|_next/image|favicon.ico|sitemap.xml|robots.txt).*)',
      ],
  };   