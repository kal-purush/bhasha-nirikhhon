import { NextResponse } from "next/server";

export async function POST(request: Request) {
  try {
    const { email, password, name, birthdate, phone } = await request.json();

    const apiUrl = "http://15.165.54.182:8080/users";

    const response = await fetch(apiUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      credentials: "include",
      body: JSON.stringify({
        email,
        password,
        name,
        birthdate,
        phone,
      }),
    });

    // 백엔드에서 반환된 응답 데이터를 모두 읽음
    const responseData = await response.json();

    if (!response.ok) {
      // 백엔드에서 보낸 에러 메시지를 포함한 응답을 클라이언트로 전송
      return NextResponse.json(responseData, { status: response.status });
    }

    // 성공한 경우에도 백엔드에서 보낸 응답 데이터를 그대로 클라이언트로 전송
    return NextResponse.json(responseData, { status: 200 });
  } catch (error: any) {
    return NextResponse.json(
      { error: "회원가입 중 오류가 발생했습니다: " + error.message },
      { status: 500 }
    );
  }
}