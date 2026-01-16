import { NextResponse } from "next/server";
import prisma from "@/lib/prisma";
import { getUserFromRequest } from "@/lib/auth";
import { ProposalStatus, Prisma } from "@prisma/client";
import type { UserMetadata } from "@/types/consideration";

interface FormattedProposal {
  id: number;
  proposalName: string;
  abstract: string;
  budgetRequest: number;
  createdAt: Date;
  status: ProposalStatus;
  submitter: string;
}

// Type for the raw proposal from database
interface RawProposal {
  id: number;
  proposalName: string;
  abstract: string;
  budgetRequest: Prisma.Decimal;
  createdAt: Date;
  status: ProposalStatus;
  user: {
    metadata: Prisma.JsonValue;
  };
}

// Type guard to check if the metadata has the required username field
function hasUsername(metadata: Prisma.JsonValue): metadata is { username: string } {
  return (
    typeof metadata === 'object' &&
    metadata !== null &&
    'username' in metadata &&
    typeof (metadata as { username: string }).username === 'string'
  );
}

// Helper function to safely get username from metadata
function getUsernameFromMetadata(metadata: Prisma.JsonValue): string {
  if (hasUsername(metadata)) {
    return metadata.username;
  }
  return 'Anonymous';
}

export async function GET(
  request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const user = await getUserFromRequest(request);
    if (!user) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const fundingRoundId = (await params).id;

    // Get all proposals for this funding round
    const proposals = await prisma.proposal.findMany({
      where: {
        fundingRoundId,
        status: ProposalStatus.CONSIDERATION,
      },
      include: {
        user: {
          select: {
            metadata: true
          }
        }
      },
      orderBy: {
        createdAt: 'desc'
      }
    });

    // Transform the proposals to include user information
    const formattedProposals = proposals.map((proposal: RawProposal): FormattedProposal => ({
      id: proposal.id,
      proposalName: proposal.proposalName,
      abstract: proposal.abstract,
      budgetRequest: proposal.budgetRequest.toNumber(),
      createdAt: proposal.createdAt,
      status: proposal.status,
      submitter: getUsernameFromMetadata(proposal.user.metadata)
    }));

    return NextResponse.json(formattedProposals);
  } catch (error) {
    console.error("Failed to fetch proposals:", error);
    return NextResponse.json(
      { error: "Internal server error" },
      { status: 500 }
    );
  }
} 