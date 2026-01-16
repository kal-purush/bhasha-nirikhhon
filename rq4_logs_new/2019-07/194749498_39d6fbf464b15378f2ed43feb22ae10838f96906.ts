import { ThunkAction, ThunkDispatch } from 'redux-thunk';
import { ActionCreator } from 'redux';

import { SET_QUOTE, SET_QUOTE_ERROR } from './constants';
import { ActionSet } from './defaults';
import { Quote, fetchQuote } from '../util';

export type QuoteAction = ActionSet<Quote> | ActionSet<string>;

export const setQuote: ActionCreator<QuoteAction> = (
  payload: Quote
): QuoteAction => ({ type: SET_QUOTE, payload });

export const setQuoteError: ActionCreator<QuoteAction> = (
  payload: string
): QuoteAction => ({ type: SET_QUOTE_ERROR, payload });

export const getQuote: ActionCreator<
  ThunkAction<Promise<void>, {}, {}, QuoteAction>
> = (
  companySymbol: string
): ThunkAction<Promise<void>, {}, {}, QuoteAction> => async (
  dispatch: ThunkDispatch<{}, {}, QuoteAction>
) => {
  fetchQuote(companySymbol)
    .then(response => {
      dispatch(setQuote(response));
    })
    .catch(error => {
      dispatch(setQuoteError(error));
    });
};