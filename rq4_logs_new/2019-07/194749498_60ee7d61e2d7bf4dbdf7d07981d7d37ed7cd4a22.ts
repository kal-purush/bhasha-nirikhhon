import { ThunkAction, ThunkDispatch } from 'redux-thunk';
import { ActionCreator } from 'redux';

import { SET_NEWS, SET_NEWS_ERROR } from './constants';
import { ActionSet } from './defaults';
import { News, fetchNews } from '../util';

export type NewsAction = ActionSet<News> | ActionSet<string>;

export const setNews: ActionCreator<NewsAction> = (
  payload: News
): NewsAction => ({ type: SET_NEWS, payload });

export const setNewsError: ActionCreator<NewsAction> = (
  payload: string
): NewsAction => ({ type: SET_NEWS_ERROR, payload });

export const getNews = (
  companySymbol: string,
  last?: number
): ThunkAction<Promise<void>, {}, {}, NewsAction> => async (
  dispatch: ThunkDispatch<{}, {}, NewsAction>
) => {
  try {
    fetchNews(companySymbol, last).then(response => {
      dispatch(setNews(response));
    });
  } catch (error) {
    dispatch(setNewsError(error));
  }
};