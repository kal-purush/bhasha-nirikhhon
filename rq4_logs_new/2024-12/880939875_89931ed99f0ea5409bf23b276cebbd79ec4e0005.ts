// src/services/ConsiderationVotingService.ts

import { PrismaClient, ConsiderationDecision, ProposalStatus, ConsiderationVote } from "@prisma/client";
import { ProposalStatusMoveService } from "./ProposalStatusMoveService";
import { FundingRoundService } from "./FundingRoundService";
import logger from "@/logging";
import { UserMetadata } from "@/services";

interface VoteInput {
  proposalId: number;
  voterId: string;
  decision: ConsiderationDecision;
  feedback: string;
}

interface VoteEligibility {
  eligible: boolean;
  message?: string;
}

interface VoteQueryResult {
  proposal: {
    id: number;
    status: ProposalStatus;
    proposalName: string;
    abstract: string;
    createdAt: Date;
    user: {
      metadata: UserMetadata;
    };
    considerationVotes: ConsiderationVote[];
  };
  voter: {
    metadata: UserMetadata;
  };
}

const voteIncludeQuery = {
  proposal: {
    select: {
      id: true,
      status: true,
      proposalName: true,
      abstract: true,
      createdAt: true,
      user: {
        select: {
          metadata: true
        }
      },
      considerationVotes: true
    }
  },
  voter: {
    select: {
      metadata: true
    }
  }
} as const;

export class ConsiderationVotingService {
  private statusMoveService: ProposalStatusMoveService;
  private fundingRoundService: FundingRoundService;

  constructor(private prisma: PrismaClient) {
    this.statusMoveService = new ProposalStatusMoveService(prisma);
    this.fundingRoundService = new FundingRoundService(prisma);
  }

  async submitVote(input: VoteInput): Promise<VoteQueryResult> {
    const existingVote = await this.getVoteWithDetails(input.proposalId, input.voterId);
    
    // Create or update vote
    const vote = await this.createOrUpdateVote(input, existingVote);
    
    // Check if status should change and refresh data
    await this.statusMoveService.checkAndMoveProposal(input.proposalId);
    
    // Get fresh data after potential status change
    return this.refreshVoteData(input.proposalId, input.voterId);
  }

  async checkVotingEligibility(
    proposalId: number, 
    fundingRoundId: string
  ): Promise<VoteEligibility> {
    const fundingRound = await this.fundingRoundService.getFundingRoundById(fundingRoundId);
    
    if (!fundingRound || fundingRound === null) {
      return { eligible: false, message: "Funding round not found" };
    }

    // Ensure all required phases exist
    if (!fundingRound.submissionPhase || 
        !fundingRound.considerationPhase || 
        !fundingRound.deliberationPhase || 
        !fundingRound.votingPhase) {
      return { 
        eligible: false, 
        message: "Funding round is not properly configured" 
      };
    }

    // Check current phase
    const currentPhase = this.fundingRoundService.getCurrentPhase(fundingRound);
    if (currentPhase !== 'consideration') {
      return { 
        eligible: false, 
        message: "Voting is only allowed during the consideration phase" 
      };
    }

    // Get proposal status
    const proposal = await this.prisma.proposal.findUnique({
      where: { id: proposalId }
    });

    if (!proposal) {
      return { eligible: false, message: "Proposal not found" };
    }

    // Allow voting if proposal is in CONSIDERATION or DELIBERATION status
    if (proposal.status !== ProposalStatus.CONSIDERATION && 
        proposal.status !== ProposalStatus.DELIBERATION) {
      return { 
        eligible: false, 
        message: "Proposal is not eligible for consideration votes" 
      };
    }

    return { eligible: true };
  }

  async checkReviewerEligibility(userId: string, proposalId: number): Promise<boolean> {
    const proposal = await this.prisma.proposal.findUnique({
      where: { id: proposalId },
      include: {
        fundingRound: {
          include: {
            topic: {
              include: {
                reviewerGroups: {
                  include: {
                    reviewerGroup: {
                      include: {
                        members: true
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    });

    if (!proposal?.fundingRound?.topic?.reviewerGroups) {
      return false;
    }

    return proposal.fundingRound.topic.reviewerGroups.some(trg =>
      trg.reviewerGroup.members.some(member => 
        member.userId === userId
      )
    );
  }

  private async getVoteWithDetails(
    proposalId: number, 
    voterId: string
  ): Promise<VoteQueryResult | null> {
    return this.prisma.considerationVote.findUnique({
      where: {
        proposalId_voterId: { proposalId, voterId }
      },
      include: voteIncludeQuery
    });
  }

  private async createOrUpdateVote(
    input: VoteInput,
    existingVote: VoteQueryResult | null
  ): Promise<VoteQueryResult> {
    if (existingVote) {
      return this.prisma.considerationVote.update({
        where: {
          proposalId_voterId: {
            proposalId: input.proposalId,
            voterId: input.voterId
          }
        },
        data: {
          decision: input.decision,
          feedback: input.feedback,
        },
        include: voteIncludeQuery
      });
    }

    return this.prisma.considerationVote.create({
      data: {
        proposalId: input.proposalId,
        voterId: input.voterId,
        decision: input.decision,
        feedback: input.feedback,
      },
      include: voteIncludeQuery
    });
  }

  private async refreshVoteData(
    proposalId: number, 
    voterId: string
  ): Promise<VoteQueryResult> {
    const vote = await this.prisma.considerationVote.findUniqueOrThrow({
      where: {
        proposalId_voterId: { proposalId, voterId }
      },
      include: voteIncludeQuery
    });

    logger.info(
      `Vote refreshed for proposal ${proposalId}. Current status: ${vote.proposal.status}`
    );

    return vote;
  }
}