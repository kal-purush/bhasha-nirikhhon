import { Action } from '@ngrx/store';
import { Post } from '../models/post';
import { type } from '../util';

export const ActionTypes = {
    FETCH:          type('[Post] Fetch'),
    FETCH_SUCCESS:  type('[Post] Fetch Success')
};

export class FetchAction implements Action {
    type = ActionTypes.FETCH;

    constructor() { }
}

export class FetchSuccessAction implements Action {
    type = ActionTypes.FETCH_SUCCESS;

    constructor(public payload: Post[]) { }
}

export type Actions
    = FetchAction
    | FetchSuccessAction;