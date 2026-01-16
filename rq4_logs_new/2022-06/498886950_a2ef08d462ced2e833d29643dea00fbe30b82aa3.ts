import { NextFetchEvent, NextRequest, NextResponse } from 'next/server';
// import { jwt } from '../../utils';

export async function middleware(req: NextRequest, ev: NextFetchEvent) {
  const { token = '' } = req.cookies;

  try {
    // await jwt.isValid(token);
    return NextResponse.next();
  } catch (error) {
    const url = req.nextUrl.clone();
    console.log(url.origin);
    const requestedPage = req.page.name;
    return NextResponse.redirect(`${url.origin}/auth/login?p=${requestedPage}`);
  }
}