import { Action } from '@ngrx/store';
import { Entity } from './profile.reducer';

export enum ProfileActionTypes {
  LoadProfile = '[Profile] Load Profile',
  ProfileLoaded = '[Profile] Profile Loaded',
  ProfileLoadError = '[Profile] Profile Load Error'
}

export class LoadProfile implements Action {
  readonly type = ProfileActionTypes.LoadProfile;
}

export class ProfileLoadError implements Action {
  readonly type = ProfileActionTypes.ProfileLoadError;
  constructor(public payload: any) {}
}

export class ProfileLoaded implements Action {
  readonly type = ProfileActionTypes.ProfileLoaded;
  constructor(public payload: Entity[]) {}
}

export type ProfileAction = LoadProfile | ProfileLoaded | ProfileLoadError;

export const fromProfileActions = {
  LoadProfile,
  ProfileLoaded,
  ProfileLoadError
};