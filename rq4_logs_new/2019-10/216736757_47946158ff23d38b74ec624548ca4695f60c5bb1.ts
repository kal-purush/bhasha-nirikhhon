import { Action } from '@ngrx/store';
import { type } from '../../utility';

export const ActionTypes = {
    GET_USER_DETAIL: type('[COMM]GET_USER_DETAIL'),
    GET_USER_DETAIL_SUCCESS: type('[COMM]GET_USER_DETAIL_SUCCESS'),
    GET_USER_DETAIL_FAIL: type('[COMM]GET_USER_DETAIL_FAIL')
}

/**
 * Common Action
 */
export class GetUserDetailAction implements Action {
    type = ActionTypes.GET_USER_DETAIL
    constructor(public payload: any) { }
}

export class GetUserDetailSuccessAction implements Action {
    type = ActionTypes.GET_USER_DETAIL_SUCCESS
    constructor(public payload: any) { }
}

export class GetUserDetailFailAction implements Action {
    type = ActionTypes.GET_USER_DETAIL_FAIL
    constructor(public payload: any = null) { }
}


export type Actions
    = GetUserDetailAction
    | GetUserDetailSuccessAction
    | GetUserDetailFailAction