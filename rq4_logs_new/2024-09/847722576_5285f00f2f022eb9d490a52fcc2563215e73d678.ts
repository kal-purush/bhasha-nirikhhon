import { NextResponse } from "next/server";

export async function GET() {
  try {
    const accessToken = process.env.NEXT_SERVER_JWT_TEST as string;
    const resp = await fetch(
      "http://localhost:5000/api/get-discounts?type=DISCOUNT",
      {
        method: "GET",
        headers: {
          "Content-Type": "application/json",
          Authorization: accessToken,
        },
      }
    );
    const { success, result } = await resp.json();
    if (!success) {
      throw new Error("Failed to fetch discounts");
    }
    return NextResponse.json({ result }, { status: 200 });
  } catch (error) {
    console.log(error);
    return NextResponse.json({ error }, { status: 500 });
  }
}