import { NextResponse } from "next/server";

export async function POST(request: Request) {
  try {
    // FormData를 사용하여 파일을 처리합니다.
    const formData = await request.formData();
    const file = formData.get("file") as Blob;
    const groupId = formData.get("groupId"); // 추가된 필드

    if (!file) {
      return NextResponse.json({ error: "No file uploaded" }, { status: 400 });
    }

    // FormData 객체를 새로 생성하고, 파일과 groupId를 추가합니다.
    const uploadData = new FormData();
    uploadData.append("file", file);
    if (groupId) {
      uploadData.append("groupId", groupId.toString());
    }

    // 예를 들어, 다른 서버로 파일을 전송하는 경우:
    const apiUrl = "http://15.165.54.182:8080/diaries/create";

    const response = await fetch(apiUrl, {
      method: "POST",
      body: uploadData, // FormData 자체를 전송
    });

    if (!response.ok) {
      const responseData = await response.json();
      return NextResponse.json(responseData, { status: response.status });
    }

    const responseData = await response.json();

    return NextResponse.json(responseData, { status: 200 });
  } catch (error: any) {
    return NextResponse.json(
      { error: "파일 업로드 중 오류가 발생했습니다: " + error.message },
      { status: 500 }
    );
  }
}