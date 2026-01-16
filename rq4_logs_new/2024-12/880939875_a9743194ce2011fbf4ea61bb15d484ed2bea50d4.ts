import logger from "@/logging";

interface OCVVote {
  account: string;
  hash: string;
  memo: string;
  height: number;
  status: string;
  timestamp: number;
  nonce: number;
}

export interface OCVVoteResponse {
  proposal_id: number;
  total_community_votes: number;
  total_positive_community_votes: number;
  total_negative_community_votes: number;
  total_stake_weight: string;
  positive_stake_weight: string;
  negative_stake_weight: string;
  vote_status: string;
  eligible: boolean;
  votes: OCVVote[];
}

export class OCVApiService {
  private baseUrl: string;

  constructor() {
    this.baseUrl = process.env.NEXT_PUBLIC_OCV_API_BASE_URL!;
  }

  async getConsiderationVotes(
    proposalId: number,
    startTime: number,
    endTime: number
  ): Promise<OCVVoteResponse> {
    const url = `${this.baseUrl}/api/mef_proposal_consideration/${proposalId}/${startTime}/${endTime}`;
    
    try {
      const response = await fetch(url, { 
        headers: {
          'Accept': 'application/json'
        },
        signal: AbortSignal.timeout(10000)
      });

      if (!response.ok) {
        throw new Error(`OCV API error: ${response.statusText}`);
      }

      const data = await response.json();
      logger.debug(`OCV vote data for proposal ${proposalId}:`, data);
      
      return data;
    } catch (error) {
      logger.error(`Failed to fetch OCV votes for proposal ${proposalId}:`, error);
      throw error;
    }
  }
} 