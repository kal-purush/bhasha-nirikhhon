import { NextResponse } from "next/server";
import prisma from "@/lib/prisma";
import { getOrCreateUserFromRequest } from "@/lib/auth";
import { AdminService } from "@/services/AdminService";
import { ApiResponse } from "@/lib/api-response";
import { AppError } from "@/lib/errors";
import { AuthErrors } from "@/constants/errors";
import { UserMetadata } from "@/services";

const adminService = new AdminService(prisma);

export async function GET(
  request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const user = await getOrCreateUserFromRequest(request);
    if (!user) {
      throw AppError.unauthorized(AuthErrors.UNAUTHORIZED);
    }

    // Check if user is admin
    const isAdmin = await adminService.checkAdminStatus(user.id, user.linkId);
    if (!isAdmin) {
      throw AppError.forbidden(AuthErrors.FORBIDDEN);
    }

    // Get funding round ID from params
    const fundingRoundId = (await params).id;

    // Verify funding round exists
    const fundingRound = await prisma.fundingRound.findUnique({
      where: { id: fundingRoundId },
    });

    if (!fundingRound) {
      throw AppError.notFound("Funding round not found");
    }

    // Get proposals for the funding round
    const proposals = await prisma.proposal.findMany({
      where: {
        fundingRoundId,
      },
      include: {
        user: {
          select: {
            metadata: true,
          },
        },
        fundingRound: {
          select: {
            name: true,
          },
        },
      },
      orderBy: [
        { status: "asc" },
        { createdAt: "desc" },
      ],
    });

    // Transform the data for the frontend
    const transformedProposals = proposals.map(proposal => ({
      id: proposal.id,
      proposalName: proposal.proposalName,
      status: proposal.status,
      budgetRequest: proposal.budgetRequest,
      createdAt: proposal.createdAt,
      submitter: (proposal.user?.metadata as UserMetadata)?.username || "Unknown",
      fundingRound: proposal.fundingRound?.name,
    }));

    return ApiResponse.success(transformedProposals);
  } catch (error) {
    return ApiResponse.error(error);
  }
} 