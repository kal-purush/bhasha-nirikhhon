import { Action } from '@ngrx/store';

export enum CompanyActionTypes {
  GET_COMPANIES = '[Companies] Get all',
  GET_COMPANIES_FAILURE = '[Companies] Get all failure',
  GET_COMPANIES_SUCCESS = '[Companies] Get all success'
}

export class GetCompany implements Action {
  readonly type = CompanyActionTypes.GET_COMPANIES;
  constructor() {}
}

export class GetCompanySuccess implements Action {
  readonly type = CompanyActionTypes.GET_COMPANIES_SUCCESS;
  constructor(public payload: { companyList: Company[] }) {}
}

export class GetCompanyFailure implements Action {
  readonly type = CompanyActionTypes.GET_COMPANIES_FAILURE;
  constructor(public payload: { error: string }) {}
}

export type CompanyActions = GetCompany | GetCompanySuccess | GetCompanyFailure;