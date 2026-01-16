import { NextResponse } from "next/server";

export async function GET(request: Request) {
  try {
    const apiUrl = `${process.env.NEXT_BACK_SERVER}/diary`;

    const cookieHeader = request.headers.get("cookie");

    const response = await fetch(apiUrl, {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        Cookie: cookieHeader || "",
      },
      credentials: "include",
    });

    const responseData = await response.json();

    if (!response.ok) {
      return NextResponse.json(responseData, { status: response.status });
    }

    return NextResponse.json(responseData, { status: 200 });
  } catch (error: any) {
    return NextResponse.json(
      { error: "그림일기 조회 중 오류가 발생했습니다: " + error.message },
      { status: 500 }
    );
  }
}