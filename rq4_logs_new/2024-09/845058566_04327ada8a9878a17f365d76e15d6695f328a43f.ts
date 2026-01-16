import { NextRequest, NextResponse } from "next/server";
import axios from 'axios';
import logger from "@/utilities/logger";

export async function POST(request: NextRequest) {
  try {
    const jsonRequest = await request.json();
    const loginResponse = await axios.post('http://localhost:8086/identity/token', jsonRequest);
    return NextResponse.json({ data: loginResponse.data }, {status: 200});          
  } catch (error) {
    if (axios.isAxiosError(error)) {
      logger.error(`Error occurred while login process.`);
      const statusCode = error.response?.status;
      const responseBody = error.response?.data;
      return NextResponse.json(
        {
          error: responseBody,
        },
        { status: statusCode }
      );
    } 
    else {
      const err = error as Error;
      logger.error(`Error occurred while login process.`);
      return NextResponse.json(
          { error: err.message },
          { status: 500 }
      );
      
    }
  }
}