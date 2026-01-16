import { leaderboard } from 'api/leaderboard';
import { LeaderboardResponse, LeaderboardResponseItem } from 'api/types';
import { setNotificationError } from 'utils';
import { AppThunkAction } from '../types';
import { SET_LEADERBOARD } from 'store/consts';
import avatar from 'assets/anonymous.png';

export const getLeaderboard = (): AppThunkAction<string> => async (dispatch) => {
  try {
    const leaders = (await leaderboard.getAll()) as LeaderboardResponse;

    leaders.forEach((item: LeaderboardResponseItem) => {
      if (item.data.avatar === undefined) {
        // eslint-disable-next-line no-param-reassign
        item.data.avatar = avatar;
      }
      if (item.data.login === undefined) {
        // eslint-disable-next-line no-param-reassign
        item.data.login = 'Guest';
      }
    });

    dispatch({ type: SET_LEADERBOARD, payload: leaders });

    return leaders;
  } catch (error) {
    setNotificationError(error);
  }
};