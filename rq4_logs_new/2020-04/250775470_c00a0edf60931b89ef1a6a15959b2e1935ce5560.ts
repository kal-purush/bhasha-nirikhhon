import User from '../../../shared/model/user.model';

export enum UserActionTypes {
  LoadUser = '[User] Load',
  LoadUserSuccess = '[User] Load Success',
  LoadUserFailure = '[User] Load Failure',
  ResetUser = '[User] Reset'
}

export class LoadUser {
  readonly type = UserActionTypes.LoadUser;
}

export class LoadUserSuccess {
  readonly type = UserActionTypes.LoadUserSuccess;

  constructor(public payload: User) {}
}

export class LoadUserFailure {
  readonly type = UserActionTypes.LoadUserFailure;
}

export class ResetUser {
  readonly type = UserActionTypes.ResetUser;
}

export type UserActionUnion = LoadUser | ResetUser | LoadUserSuccess | LoadUserFailure;