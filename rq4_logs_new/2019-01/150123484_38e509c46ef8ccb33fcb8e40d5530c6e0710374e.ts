import { Action } from '@ngrx/store';
import { IListings } from './../types/listings-state.interface';

export enum ListingsActionTypes {
  GetListings = '[Listings] Get Listings',
  GetListingsSuccess = '[Listings] Get Listings Success',
  GetListingsFailure = '[Listings] Get Listings Failure'
}

export class GetListings implements Action {
  readonly type = ListingsActionTypes.GetListings;
}

export class GetListingsSuccess implements Action {
  readonly type = ListingsActionTypes.GetListingsSuccess;
  constructor(public payload: IListings) {}
}

export class GetListingsFailure implements Action {
  readonly type = ListingsActionTypes.GetListingsFailure;
  constructor(public payload: Error) {}
}

export type ListingsActionsUnion =
  | GetListings
  | GetListingsSuccess
  | GetListingsFailure;