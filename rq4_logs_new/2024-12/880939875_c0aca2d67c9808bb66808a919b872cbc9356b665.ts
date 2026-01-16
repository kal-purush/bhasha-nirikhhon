/**
 * In-development verstion of /api/me/info
 * 
 * Later, this will replace /api/me/info and its usages.
 */
import { NextResponse } from "next/server";
import { cookies } from "next/headers";
import prisma from "@/lib/prisma";
import { verifyToken } from "@/lib/auth/jwt";
import { UserService } from "@/services/UserService";
import logger from "@/logging";
import { deriveUserId } from "@/lib/user/derive";
import { ApiResponse } from "@/lib/api-response";

const userService = new UserService(prisma);

export async function GET() {
  try {
    const cookieStore = await cookies();
    const accessToken = cookieStore.get("access_token")?.value;

    if (!accessToken) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    // Verify JWT and get payload
    const payload = await verifyToken(accessToken);
    
    // Get user ID
    const userId = deriveUserId(payload.authSource);

    // Get complete user info
    const userInfo = await userService.getUserInfo(userId);
    
    if (!userInfo) {
      return NextResponse.json({ error: "User not found" }, { status: 404 });
    }

    return ApiResponse.success(userInfo);

  } catch (error) {
    logger.error("User info error:", error);

    if (error instanceof Error && error.message === "Invalid token") {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    return NextResponse.json(
      { error: "Internal server error" },
      { status: 500 }
    );
  }
}