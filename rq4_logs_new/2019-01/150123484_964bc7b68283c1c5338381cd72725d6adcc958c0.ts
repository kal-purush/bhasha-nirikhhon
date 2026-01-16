import { Action } from '@ngrx/store';
import { IProfile } from '../../../types/app-state/app-state.interface';

export enum ProfileActionTypes {
  UpdateProfile = '[Profile] Update Profile',
  UpdateProfileSuccess = '[Profile] Update Profile Success',
  UpdateProfileFailure = '[Profile] Update Profile Failure'
}

export class UpdateProfile implements Action {
  readonly type = ProfileActionTypes.UpdateProfile;
}

export class UpdateProfileSuccess implements Action {
  readonly type = ProfileActionTypes.UpdateProfileSuccess;
  constructor(public payload: IProfile) {}
}

export class UpdateProfileFailure implements Action {
  readonly type = ProfileActionTypes.UpdateProfileFailure;
  constructor(public payload: Error) {}
}