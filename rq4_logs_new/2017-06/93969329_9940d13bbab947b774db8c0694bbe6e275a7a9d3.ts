import { Action } from '@ngrx/store';
import { Good } from '../models/good';

export const LOAD_COLLECTION          = '[Good] Load collection';
export const LOAD_COLLECTION_SUCCESS  = '[Good] Load collection complete';
export const LOAD_COLLECTION_FAIL     = '[Good] Load collection fail';


/**
 * Every action is comprised of at least a type and an optional
 * payload. Expressing actions as classes enables powerful
 * type checking in reducer functions.
 *
 * See Discriminated Unions: https://www.typescriptlang.org/docs/handbook/advanced-types.html#discriminated-unions
 */
export class LoadCollectionAction implements Action {
  readonly type = LOAD_COLLECTION;
}

export class LoadCollectionSuccessAction implements Action {
  readonly type = LOAD_COLLECTION_SUCCESS;

  constructor(public payload: Good[]) { }
}

export class LoadCollectionFailAction implements Action {
  readonly type = LOAD_COLLECTION_FAIL;

  constructor(public payload: any) { }
}

/**
 * Export a type alias of all actions in this action group
 * so that reducers can easily compose action types
 */
export type Actions
  = LoadCollectionAction
  | LoadCollectionSuccessAction
  | LoadCollectionFailAction;
// export type Actions
//   = SearchAction
//   | SearchCompleteAction
//   | LoadAction
//   | SelectAction;