import { NextResponse } from "next/server";
import { verifyToken } from "@/lib/auth/jwt";
import prisma from "@/lib/prisma";
import logger from "@/logging";
import { AppError } from "@/lib/errors";
import { ApiResponse } from "@/lib/api-response";
import { deriveUserId } from "@/lib/user/derive";
import { UserService } from "@/services/UserService";

const userService = new UserService(prisma);

export const runtime = "nodejs";

interface LinkAccountRequest {
  walletToken: string;
  existingToken: string;
}

export async function POST(request: Request) {
  try {
    const body: LinkAccountRequest = await request.json();
    const { walletToken, existingToken } = body;

    if (!walletToken || !existingToken) {
      throw new AppError("Missing required tokens", 400);
    }

    // Verify both tokens
    const [walletPayload, existingPayload] = await Promise.all([
      verifyToken(walletToken),
      verifyToken(existingToken),
    ]);

    if (walletPayload.authSource.type !== "wallet") {
      throw new AppError("Invalid wallet token", 400);
    }

    // Derive user IDs from auth sources
    const walletUserId = deriveUserId(walletPayload.authSource);
    const existingUserId = deriveUserId(existingPayload.authSource);

    // Check if linking is possible
    const canLink = await userService.canLink(walletUserId, existingUserId);
    if (!canLink) {
      throw new AppError("Cannot link these accounts", 400);
    }

    // Link the accounts
    const linked = await userService.linkAccounts(walletUserId, existingUserId);
    if (!linked) {
      throw new AppError("Failed to link accounts", 400);
    }

    return ApiResponse.success({
      data: { message: "Accounts linked successfully" }
    });

  } catch (error) {
    logger.error("Account linking error:", error);

    if (error instanceof AppError) {
      return ApiResponse.error(error);
    }

    return ApiResponse.error({
      message: "Failed to link accounts",
      statusCode: 500
    });
  }
} 