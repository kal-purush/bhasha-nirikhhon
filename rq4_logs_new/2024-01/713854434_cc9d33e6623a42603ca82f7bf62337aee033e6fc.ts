import connect from "@/utils/db";
import Hackathon from "@/models/Hackathon";
import { NextResponse } from "next/server";

export async function POST(request: any) {
    await connect();
  const  ids  = await new Response(request.body).json();

  if (!ids || !ids.length) {
    return NextResponse.json("No ids provided in the request body");
  }
  try {
    const hackathons = await Hackathon.find({ _id: { $in: ids } });

    return NextResponse.json({ hackathons });
  } catch (error) {
    console.error("Error fetching users:", error);
    return NextResponse.json("Internal Server Error");
  }
}