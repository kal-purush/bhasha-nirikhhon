import { cookies } from 'next/headers'
import { NextRequest, NextResponse } from 'next/server'

import { createSessionCookie } from '@/lib/firebase/firebase-admin'
import { APIResponse } from '@/types/ApiResponse'

export async function POST(request: NextRequest) {
  const reqBody = (await request.json()) as { idToken: string }
  const idToken = reqBody.idToken

  const expiresIn = 60 * 60 * 24 * 5 * 1000 // 5 days

  const sessionCookie = await createSessionCookie(idToken, { expiresIn })

  cookies().set('__session', sessionCookie, {
    maxAge: expiresIn,
    httpOnly: true,
    secure: true,
  })

  // セッションクッキーが正しく設定されているかどうかを確認
  const setCookie = cookies().get('__session')
  if (setCookie) {
    console.log('Session cookie set successfully:', setCookie.value)
  } else {
    console.log('Session cookie not set')
  }

  return NextResponse.json<APIResponse<string>>({
    success: true,
    data: 'Signed in successfully.',
  })
}