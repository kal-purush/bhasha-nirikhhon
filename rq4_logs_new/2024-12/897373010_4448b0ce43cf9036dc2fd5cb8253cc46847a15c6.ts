import { NextResponse } from "next/server";

// http://localhost:3000/api/
export function GET() {
    return NextResponse.json({
        message: 'Next.js API v1.0.0 running...'
    });
}