import { NextRequest } from "next/server";
import { getOrCreateUserFromRequest } from "@/lib/auth";
import { DeliberationService } from "@/services/DeliberationService";
import { ApiResponse } from "@/lib/api-response";
import { AppError } from "@/lib/errors";
import prisma from "@/lib/prisma";
import logger from "@/logging";

const deliberationService = new DeliberationService(prisma);

export async function POST(
  request: NextRequest,
  { params }: { params: Promise<{ proposalId: string }> }
) {
  try {
    const user = await getOrCreateUserFromRequest(request);
    if (!user) {
      return ApiResponse.unauthorized("Please log in to submit deliberation");
    }

    const { feedback, recommendation } = await request.json();
    
    if (!feedback?.trim()) {
      throw new AppError("Feedback is required", 400);
    }

    const vote = await deliberationService.submitDeliberation(
      parseInt((await params).proposalId),
      user.id,
      feedback,
      recommendation
    );

    return ApiResponse.success(vote);
  } catch (error) {
    logger.error("Error submitting deliberation vote:", error);
    return ApiResponse.error(error);
  }
} 