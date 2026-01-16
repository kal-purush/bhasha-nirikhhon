import { loginUser } from "@/data/User.data";
import { LoginDto } from "@/lib/dto/User.dto";
import { loginAuth } from "@/lib/utils/auth.utils";
import { NextResponse } from "next/server";

export async function POST(req: Request) {
  try {
    const body = (await req.json()) as LoginDto
    const responseData = await loginUser(body)
    if(!responseData){
      return NextResponse.json(
        { message: "User not found" },
        { status: 404 }
      )
    }
    
    const res =  NextResponse.json(responseData, { status: 200 })
    const session = await loginAuth(responseData)
    res.cookies.set("session", session, {
        httpOnly: true,
        secure: process.env.NODE_ENV === "production", // Secure in production
        maxAge: 86400, // 1 day
        path: "/",
    });

    return res;
  } catch (error) {
    return NextResponse.json(
      { message: "Internal Server Error" },
      { status: 500 }
    )
  }
}