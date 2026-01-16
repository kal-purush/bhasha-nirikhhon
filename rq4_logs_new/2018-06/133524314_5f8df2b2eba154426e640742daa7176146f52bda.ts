import { Action } from '@ngrx/store';

export const GET_PROVIDERS = '[Get] Provider';
export const GET_PROVIDERS_SUCCESS = '[Get] Provider Success';
export const GET_PROVIDERS_FAILED = '[Get] Provider Failed';


export class GetProviders implements Action {
  readonly type = GET_PROVIDERS;
  constructor(public payload: any = null) { }
}

export class GetProvidersSuccess implements Action {
  readonly type = GET_PROVIDERS_SUCCESS;
  constructor(public payload: any) { }
}

export class GetProvidersFailed implements Action {
  readonly type = GET_PROVIDERS_FAILED;
  constructor(public payload: any) { }
}

export type Actions = GetProviders | GetProvidersSuccess | GetProvidersFailed;
