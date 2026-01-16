// give the code for a next 13 route handler

import { getAccessTokenFromCookie, uploadActivity } from "@/lib/strava";

export const dynamic = "force-dynamic"; // defaults to auto
export const runtime = "edge";

export async function GET(request: Request, response: Response) {
  try {
    const hehe = await uploadActivity();
    return new Response(JSON.stringify({ res: "success" }), {
      status: 200,
      headers: {
        "Content-Type": "application/json",
      },
    });
  } catch (error) {
    return new Response(JSON.stringify({ res: "fail" }), {
      status: 500,
      headers: {
        "Content-Type": "application/json",
      },
    });
  }
}