import { PrismaClient, Prisma, CommunityDeliberationVote, ReviewerDeliberationVote } from "@prisma/client";
import { AppError } from "@/lib/errors";
import { UserMetadata } from "./UserService";

type ProposalWithVotes = Prisma.ProposalGetPayload<{
  include: {
    user: {
      select: {
        metadata: true;
      };
    };
    deliberationCommunityVotes: true;
    deliberationReviewerVotes: true;
    fundingRound: {
      include: {
        topic: {
          include: {
            reviewerGroups: {
              include: {
                reviewerGroup: {
                  include: {
                    members: true;
                  };
                };
              };
            };
          };
        };
      };
    };
  };
}>;

export class DeliberationService {
  constructor(private prisma: PrismaClient) {}

  async getDeliberationProposals(fundingRoundId: string, userId: string) {
    if (!fundingRoundId) {
      throw new AppError("Funding round ID is required", 400);
    }

    const fundingRound = await this.prisma.fundingRound.findUnique({
      where: { id: fundingRoundId },
      include: {
        deliberationPhase: true,
        topic: {
          include: {
            reviewerGroups: {
              include: {
                reviewerGroup: {
                  include: {
                    members: {
                      where: { userId },
                    },
                  },
                },
              },
            },
          },
        },
      },
    });

    if (!fundingRound) {
      throw new AppError("Funding round not found", 404);
    }

    // Get all proposals in deliberation phase for this funding round
    const proposals = await this.prisma.proposal.findMany({
      where: {
        fundingRoundId,
        status: 'DELIBERATION',
      },
      include: {
        user: {
          select: {
            metadata: true,
          },
        },
        deliberationCommunityVotes: {
          where: { userId },
          take: 1,
        },
        deliberationReviewerVotes: {
          where: { userId },
          take: 1,
        },
        fundingRound: {
          include: {
            topic: {
              include: {
                reviewerGroups: {
                  include: {
                    reviewerGroup: {
                      include: {
                        members: true,
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
      orderBy: {
        createdAt: 'desc',
      },
    });

    // Get reviewer deliberations separately
    const reviewerDeliberations = await this.prisma.reviewerDeliberationVote.findMany({
      where: {
        proposalId: {
          in: proposals.map(p => p.id),
        },
      },
      include: {
        user: {
          select: {
            metadata: true,
          },
        },
      },
      orderBy: {
        createdAt: 'desc',
      },
    });

    // Check if user is a reviewer
    const isReviewer = fundingRound.topic.reviewerGroups.some(
      group => group.reviewerGroup.members.length > 0
    );

    // Transform the data for the frontend
    const transformedProposals = proposals.map(proposal => {
      const userReviewerVote = proposal.deliberationReviewerVotes[0];
      const userCommunityVote = proposal.deliberationCommunityVotes[0];
      
      // Use reviewer vote if exists, otherwise use community vote
      const userDeliberation = userReviewerVote 
        ? {
            feedback: userReviewerVote.feedback,
            recommendation: userReviewerVote.recommendation,
            createdAt: userReviewerVote.createdAt,
            isReviewerVote: true,
          }
        : userCommunityVote
          ? {
              feedback: userCommunityVote.feedback,
              recommendation: undefined,
              createdAt: userCommunityVote.createdAt,
              isReviewerVote: false,
            }
          : null;

      return {
        id: proposal.id,
        proposalName: proposal.proposalName,
        abstract: proposal.abstract,
        submitter: (proposal.user?.metadata as UserMetadata)?.username || 'Unknown',
        isReviewerEligible: isReviewer,
        userDeliberation,
        hasVoted: Boolean(userReviewerVote || userCommunityVote),
        reviewerComments: reviewerDeliberations
          .filter(d => d.proposalId === proposal.id)
          .map(comment => ({
            id: comment.id,
            feedback: comment.feedback,
            recommendation: comment.recommendation,
            createdAt: comment.createdAt,
            reviewer: {
              username: (comment.user?.metadata as UserMetadata)?.username || 'Unknown',
            },
          })),
        createdAt: proposal.createdAt,
      };
    });

    // Calculate pending count
    const pendingCount = transformedProposals.filter(p => !p.hasVoted).length;

    return {
      proposals: transformedProposals,
      pendingCount,
      totalCount: transformedProposals.length,
    };
  }

  async submitDeliberation(
    proposalId: number,
    userId: string,
    feedback: string,
    recommendation?: boolean
  ) {
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
                        members: {
                          where: { userId },
                        },
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
    });

    if (!proposal) {
      throw new AppError("Proposal not found", 404);
    }

    if (proposal.status !== 'DELIBERATION') {
      throw new AppError("Proposal is not in deliberation phase", 400);
    }

    // Check if user is a reviewer
    const isReviewer = proposal.fundingRound?.topic.reviewerGroups.some(
      group => group.reviewerGroup.members.length > 0
    ) ?? false;

    if (recommendation !== undefined && !isReviewer) {
      throw new AppError("Only reviewers can submit recommendations", 403);
    }

    // Create or update the appropriate vote type
    if (isReviewer) {
      if (recommendation === undefined) {
        throw new AppError("Reviewers must provide a recommendation", 400);
      }

      return await this.prisma.reviewerDeliberationVote.upsert({
        where: {
          proposalId_userId: {
            proposalId,
            userId,
          },
        },
        create: {
          proposalId,
          userId,
          feedback,
          recommendation,
        },
        update: {
          feedback,
          recommendation,
        },
      });
    } else {
      return await this.prisma.communityDeliberationVote.upsert({
        where: {
          proposalId_userId: {
            proposalId,
            userId,
          },
        },
        create: {
          proposalId,
          userId,
          feedback,
        },
        update: {
          feedback,
        },
      });
    }
  }
} 