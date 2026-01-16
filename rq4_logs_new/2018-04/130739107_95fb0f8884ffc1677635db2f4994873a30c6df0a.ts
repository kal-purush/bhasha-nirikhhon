import { Action } from '@ngrx/store';

import { searchActionTypes as actionTypes } from './action.types';





export class ShiftBooksResultAction implements Action {
  readonly type = actionTypes.SHIFT_BOOK_RESULTS;

  constructor(
    public moveIndex: number
  ) {}
}