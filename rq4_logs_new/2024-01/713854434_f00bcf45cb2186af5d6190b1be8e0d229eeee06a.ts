import connect from "@/utils/db";
import User from "@/models/User";
import { NextResponse } from "next/server";
await connect();

export async function PUT(request: any) {
  try {
    const userEmail = request.nextUrl.searchParams.get("userEmail");

    const currentUser = await User.findOne({ email: userEmail });

    if (!currentUser) {
      return NextResponse.json("User not found");
    }

    const tags  = await new Response( request.body).json();
    currentUser.tags = tags;

    await currentUser.save(); 
    return NextResponse.json({ success: true });
  } catch (error) {
    console.error(error);
    return NextResponse.json("Internal Server Error");
  }
}