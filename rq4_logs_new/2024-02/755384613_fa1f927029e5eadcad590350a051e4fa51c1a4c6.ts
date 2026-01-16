import { NextResponse, type NextRequest } from "next/server";
import axios from "axios";

export async function GET(req: NextRequest) {
  try {
    const searchParams = req.nextUrl.searchParams;
    const url = searchParams.get("url");
    const response = await axios.get(url as string);
    const titleMatch = response.data.match(/<title>(.*?)<\/title>/);
    const title = titleMatch ? titleMatch[1] : "Form Title Not Found";
    return NextResponse.json({
      message: "Successfully retrieved google form",
      data: {
        url: url,
        title,
      },
    });
  } catch (error) {
    return NextResponse.error();
  }
}