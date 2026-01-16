import { Action } from '@ngrx/store';
import { ILanguage } from '../types/language-state.interface';

export enum LanguageActionTypes {
  GetLanguage = '[Language] Get Language',
  GetLanguagesSuccess = '[Language] Get Language Success',
  GetLanguagesFailure = '[Language] Get Language Failure'
}

export class GetLanguage implements Action {
  readonly type = LanguageActionTypes.GetLanguage;
}

export class GetLanguagesSuccess implements Action {
  readonly type = LanguageActionTypes.GetLanguagesSuccess;
  constructor(public payload: ILanguage) {}
}

export class GetLanguagesFailure implements Action {
  readonly type = LanguageActionTypes.GetLanguagesFailure;
  constructor(public payload: Error) {}
}

export type LanguageActionsUnion =
  | GetLanguage
  | GetLanguagesSuccess
  | GetLanguagesFailure;