import { NextRequest } from 'next/server';
import { ApiResponse } from '@/lib/api-response';
import { AppError } from '@/lib/errors';
import { prisma } from '@/lib/prisma';
import { GetRankedEligibleProposalsAPIResponse, RankedVotingService } from '@/services/RankedVotingService';

export async function GET(request: NextRequest) {
  try {
    const fundingRoundId = request.nextUrl.searchParams.get('fundingRoundId');
    
    if (!fundingRoundId) {
      throw new AppError('Funding round ID is required', 400);
    }

    const rankedVotingService = new RankedVotingService(prisma);
    const result: GetRankedEligibleProposalsAPIResponse = await rankedVotingService.getEligibleProposals(fundingRoundId);

    return ApiResponse.success(result);
  } catch (error) {
    return ApiResponse.error(error);
  }
} 