export class GetLeaderBoardEntriesServiceInput {
  constructor(
    public readonly key: string,
    public readonly groupId: number = 0,
    public readonly type: 'daily' | 'weekly' | 'monthly' = 'daily',
    public readonly offset: number = 0,
    public readonly limit: number = 10,
  ) {}
}

export class GetLeaderboardEntriesServiceOutput {
  constructor(
    public readonly entries: LeaderboardEntry[],
    public readonly totalCount: number,
    public readonly hasNextPage: boolean,
    public readonly hasPreviousPage: boolean,
  ) {}
}

export class LeaderboardEntry {
  constructor(
    public readonly rank: number,
    public readonly score: number,
    public readonly userId: number,
    public readonly nickname: string | null,
    public readonly thumbnailUrl: string | null,
  ) {}
}

export interface IGetLeaderboardEntriesService {
  execute(
    dto: GetLeaderBoardEntriesServiceInput,
  ): Promise<GetLeaderboardEntriesServiceOutput>;
}