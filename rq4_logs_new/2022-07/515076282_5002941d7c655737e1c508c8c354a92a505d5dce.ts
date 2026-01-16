import { createAction, props } from '@ngrx/store';
import { Credentials, User } from 'src/app/data/user';

export const login = createAction('[Login Form] Log In', props<{ credentials: Credentials }>());

export const loginSuccess = createAction('[API] Login Success', props<{ user: User }>());

export const loginError = createAction('[API] Login Error');

export const setRedirectUrl = createAction(
  '[Protected Page] Set Redirect Url',
  props<{ redirectUrl: string }>()
);