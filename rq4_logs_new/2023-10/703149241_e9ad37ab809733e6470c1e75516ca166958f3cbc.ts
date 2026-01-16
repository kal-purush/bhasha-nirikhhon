import axios from "axios";
import { NextRequest, NextResponse } from "next/server";

export async function GET(request: NextRequest) {
    try {
      const {data} = await axios("https://jsonplaceholder.typicode.com/users");
        console.log("[USERS_GET]", data);
      return NextResponse.json(data);
    } catch (error) {
      console.log("[USERS_GET]", error);
      return new NextResponse("Internal Error", { status: 500 });
    }
  }

  export async function POST(request: NextRequest) {
    try {
      const body = await request.json();
      console.log("[USERS_POST]", body);
      
  
      return NextResponse.json({});
    } catch (error) {
      console.log("[USERS_POST]", error);
      return new NextResponse("Internal Error", { status: 500 });
    }
  }