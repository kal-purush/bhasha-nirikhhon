import connect from "@/utils/db";
import User from "@/models/User";
import { NextResponse } from "next/server";

export async function POST(request: any) {
    await connect();
  const  usernames  = await new Response(request.body).json();

  if (!usernames || !usernames.length) {
    return NextResponse.json("No usernames provided in the request body");
  }
  console.log(usernames)
  try {
    const users = await User.find({ username: { $in: usernames } });

    return NextResponse.json({ users });
  } catch (error) {
    console.error("Error fetching users:", error);
    return NextResponse.json("Internal Server Error");
  }
}