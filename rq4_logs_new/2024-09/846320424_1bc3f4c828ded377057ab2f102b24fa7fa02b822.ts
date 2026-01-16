import { createServerClient } from '@supabase/ssr'
import { NextResponse, type NextRequest } from 'next/server'

export async function updateSession(request: NextRequest) {
  // NextResponse 객체를 생성하여 초기 응답을 설정합니다.
  let supabaseResponse = NextResponse.next({
    request,
  })

  // Supabase 서버 클라이언트를 생성합니다.
  const supabase = createServerClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
    {
      cookies: {
        // 모든 쿠키를 가져오는 함수입니다.
        getAll() {
          return request.cookies.getAll()
        },
        // 쿠키를 설정하는 함수입니다.
        setAll(cookiesToSet) {
          // 요청 객체에 쿠키를 설정합니다.
          cookiesToSet.forEach(({ name, value, options }) => request.cookies.set(name, value))
          // 새로운 NextResponse 객체를 생성합니다.
          supabaseResponse = NextResponse.next({
            request,
          })
          // 응답 객체에 쿠키를 설정합니다.
          cookiesToSet.forEach(({ name, value, options }) =>
            supabaseResponse.cookies.set(name, value, options)
          )
        },
      },
    }
  )

  // 중요: createServerClient와 supabase.auth.getUser() 사이에 어떤 로직도 작성하지 마세요.
  // 작은 실수로 인해 사용자가 무작위로 로그아웃되는 문제를 디버깅하기 매우 어려워질 수 있습니다.
  // 인증 토큰 새로 고치기
  const {
    data: { user },
  } = await supabase.auth.getUser()

  if (
    !user &&
    !request.nextUrl.pathname.startsWith('/login') &&
    !request.nextUrl.pathname.startsWith('/auth')
  ) {
    // 사용자가 없음, 잠재적으로 사용자를 로그인 페이지로 리디렉션하여 응답
    const url = request.nextUrl.clone()
    url.pathname = '/login'
    return NextResponse.redirect(url)
  }

  // 중요: supabaseResponse 객체를 그대로 반환해야 합니다. 만약 NextResponse.next()로
  // 새로운 응답 객체를 생성하는 경우 다음을 반드시 지켜야 합니다:
  // 1. 다음과 같이 요청을 전달하세요:
  //    const myNewResponse = NextResponse.next({ request })
  // 2. 다음과 같이 쿠키를 복사하세요:
  //    myNewResponse.cookies.setAll(supabaseResponse.cookies.getAll())
  // 3. myNewResponse 객체를 필요에 맞게 변경하되, 쿠키는 변경하지 마세요!
  // 4. 마지막으로:
  //    return myNewResponse
  // 이를 지키지 않으면 브라우저와 서버가 동기화되지 않아 사용자 세션이 조기에 종료될 수 있습니다!

  return supabaseResponse
}