'use server'

import { signIn } from '@/auth'
import { redirect } from 'next/navigation'

export const signInWithCredentials = async (
  state: { message: string } = { message: '' },
  formData: FormData
) => {
  try {
    await signIn('credentials', {
      email: formData.get('email') || '',
      password: formData.get('password') || '',

      // redirectTo는 try 문 안에서 동작하지 않지만 명시적으로 redirect 호출해야 함
      redirect: false, // false로 해두고 아래에서 직접 처리하기
    })
  } catch (error) {
    const e = error as Error & {
      cause?: { err?: { message?: string } }
    }

    const errorCode = e?.cause?.err?.message ?? ''

    switch (errorCode) {
      case 'USER_NOT_FOUND':
        return { ...state, message: '가입되지 않은 이메일 주소입니다.' }
      case 'NOT_VALID_PASSWORD':
        return { ...state, message: '비밀번호가 일치하지 않습니다.' }
      case 'UNEXPECTED_ERROR':
        throw new Error('UNEXPECTED_ERROR')
      // 코드 오류나 프레임워크 내부 예외 등 완전히 예상치 못한 예외 (ex. NEXT_REDIRECT, CallbackRouteError, ReferenceError 등)
      default:
        console.error(error)
        // TODO: error.tsx 제대로 구현 후 error도 넘겨주게 변경
        throw new Error('UNHANDLED_ERROR')
    }
  }
  redirect('/') // 로그인 성공시 홈으로 리디렉션
}