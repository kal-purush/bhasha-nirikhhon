import { post } from 'api/http';
import { LeaderboardResponse } from 'api/types';
import { baseUrl, headersJSON as headers } from 'common';
import store from 'store';

export const leaderboard = {
  getAll(): Promise<LeaderboardResponse> {
    return post(
      `${baseUrl}/leaderboard/all`,
      {
        ratingFieldName: 'casablanca_score',
        cursor: 0,
        limit: 20,
      },
      { headers }
    );
  },

  save(score: number, level: number): Promise<LeaderboardResponse> {
    const { user } = store.getState();

    return post(
      `${baseUrl}/leaderboard`,
      {
        data: {
          casablanca_score: score,
          level,
          login: user.login,
          avatar: user.avatar,
        },
        ratingFieldName: 'casablanca_score',
      },
      { headers }
    );
  },
};