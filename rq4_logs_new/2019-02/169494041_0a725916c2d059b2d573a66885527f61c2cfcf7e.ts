import { Injectable } from '@angular/core';
import { createSelector, select, Store } from '@ngrx/store';
import { Actions, Effect, ofType } from '@ngrx/effects';
import { Observable } from 'rxjs';
import { tap, map, catchError, switchMap } from 'rxjs/operators';
import { RequestMessage, ResponseMessage } from '../models/message';
import { SetErrorApp } from './app.store';
import { UserProvider } from '../providers/user/user';

export interface UserToken {
  token: string;
}

export interface UserCredentials {
  login: string;
  password: string;
}

export interface UserProfil {
  id: string;
  email: string;
  password: string;
  username: string;
}

export interface UserState {
  authentified: boolean;
  token: UserToken;
  profil: UserProfil;
  credentials: UserCredentials;
}

export const initialUserState: UserState = {
  authentified: false,
  token: null,
  profil: null,
  credentials: null
};

export const userStateSelector = 'user';
export const selectUserState = (states: any) =>
  states[userStateSelector] as UserState;
export const selectUserAuthentified = createSelector(
  selectUserState,
  (state: UserState) => state.authentified
);
export const selectUserToken = createSelector(
  selectUserState,
  (state: UserState) => state.token
);
export const selectUserProfil = createSelector(
  selectUserState,
  (state: UserState) => state.profil
);
export const selectUserCredentials = createSelector(
  selectUserState,
  (state: UserState) => state.credentials
);

export enum UserActionType {
  signUpRequest = '[User] Sign Up Request',
  signUpResponse = '[User] Sign Up Response',
  signInRequest = '[User] Sign In Request',
  signInResponse = '[User] Sign In Response',
  autoSignInRequest = '[User] Auto Sign In Request',
  autoSignInResponse = '[User] Auto Sign In Response',
  signOutRequest = '[User] Sign Out Request',
  signOutResponse = '[User] Sign Out Response',
  getProfilRequest = '[User] Get Profil Request',
  getProfilResponse = '[User] Get Profil Response',
  updateProfilRequest = '[User] Update Profil Request',
  updateProfilResponse = '[User] Update Profil Response'
}

export class SignUpRequestUser extends RequestMessage<{
  profil: Partial<UserProfil>;
}> {
  type = UserActionType.signUpRequest;
}
export class SignUpResponseUser extends ResponseMessage<{
  profil: UserProfil;
}> {
  type = UserActionType.signUpResponse;
}
export class SignInRequestUser extends RequestMessage<{
  credentials: UserCredentials;
}> {
  type = UserActionType.signInRequest;
}
export class SignInResponseUser extends ResponseMessage<{
  profil: UserProfil;
  token: UserToken;
}> {
  type = UserActionType.signInResponse;
}
export class AutoSignInRequestUser extends RequestMessage<{
  credentials: UserCredentials;
}> {
  type = UserActionType.autoSignInRequest;
}
export class AutoSignInResponseUser extends ResponseMessage<{
  profil: UserProfil;
  token: UserToken;
}> {
  type = UserActionType.autoSignInResponse;
}
export class SignOutRequestUser extends RequestMessage<void> {
  type = UserActionType.signOutRequest;
}
export class SignOutResponseUser extends ResponseMessage<void> {
  type = UserActionType.signOutResponse;
}
export class GetProfilRequestUser extends RequestMessage<{ id: string }> {
  type = UserActionType.getProfilRequest;
}
export class GetProfilResponseUser extends ResponseMessage<{
  profil: UserProfil;
}> {
  type = UserActionType.getProfilResponse;
}
export class UpdateProfilRequestUser extends RequestMessage<{
  id: string;
  changes: Partial<UserProfil>;
}> {
  type = UserActionType.updateProfilRequest;
}
export class UpdateProfilResponseUser extends ResponseMessage<{
  profil: UserProfil;
}> {
  type = UserActionType.updateProfilResponse;
}

export type UserActions =
  | SignUpRequestUser
  | SignUpResponseUser
  | SignInRequestUser
  | SignInResponseUser
  | AutoSignInRequestUser
  | AutoSignInResponseUser
  | SignOutRequestUser
  | SignOutResponseUser
  | GetProfilRequestUser
  | GetProfilResponseUser
  | UpdateProfilRequestUser
  | UpdateProfilResponseUser;

export function userStateReducer(
  state: UserState = initialUserState,
  action: UserActions
): UserState {
  switch (action.type) {
    case UserActionType.signUpResponse: {
      const p = (action as SignUpResponseUser).payload;
      return {
        ...state,
        profil: p.profil
      };
    }
    case UserActionType.autoSignInResponse:
    case UserActionType.signInResponse: {
      const p = (action as SignInResponseUser).payload;
      return {
        ...state,
        authentified: true,
        profil: p.profil,
        token: p.token
      };
    }
    case UserActionType.signOutResponse: {
      const p = (action as SignOutResponseUser).payload;
      return {
        ...state,
        authentified: false,
        profil: null,
        token: null
      };
    }
    case UserActionType.getProfilResponse: {
      const p = (action as GetProfilResponseUser).payload;
      return {
        ...state,
        profil: p.profil
      };
    }
    case UserActionType.updateProfilResponse: {
      const p = (action as UpdateProfilResponseUser).payload;
      return {
        ...state,
        profil: p.profil
      };
    }
    case UserActionType.signUpRequest:
    case UserActionType.autoSignInRequest:
    case UserActionType.signInRequest:
    case UserActionType.signOutRequest:
    case UserActionType.getProfilRequest:
    case UserActionType.updateProfilRequest:
    default:
      return state;
  }
}

@Injectable()
export class UserFacade {
  user$: Observable<UserState> = this.store.pipe(select(selectUserState));
  authentified$: Observable<boolean> = this.store.pipe(
    select(selectUserAuthentified)
  );
  token$: Observable<UserToken> = this.store.pipe(select(selectUserToken));
  profil$: Observable<UserProfil> = this.store.pipe(select(selectUserProfil));
  credentials$: Observable<UserCredentials> = this.store.pipe(
    select(selectUserCredentials)
  );
  constructor(private store: Store<any>) {}
  signUpRequest(payload: { profil: Partial<UserProfil> }) {
    this.store.dispatch(new SignUpRequestUser(payload));
  }
  signInRequest(payload: { credentials: UserCredentials }) {
    this.store.dispatch(new SignInRequestUser(payload));
  }
  signOutRequest() {
    this.store.dispatch(new SignOutRequestUser());
  }
  getProfilRequest(payload: { id: string }) {
    this.store.dispatch(new GetProfilRequestUser(payload));
  }
  updateProfilRequest(payload: { id: string; changes: Partial<UserProfil> }) {
    this.store.dispatch(new UpdateProfilRequestUser(payload));
  }
}

@Injectable()
export class UserEffects {
  constructor(
    public actions$: Actions,
    public userFacade: UserFacade,
    public user: UserProvider
  ) { }
  @Effect({ dispatch: false })
  signUpRequestLog$ = this.actions$.pipe(
    ofType(UserActionType.signUpRequest),
    tap((signUpRequest: SignUpRequestUser) => {
      console.log('UserEffects@signUpRequestLog: ', signUpRequest);
    })
  );
  @Effect({ dispatch: false })
  signUpResponseLog$ = this.actions$.pipe(
    ofType(UserActionType.signUpResponse),
    tap((signUpResponse: SignUpResponseUser) => {
      console.log('UserEffects@signUpResponseLog: ', signUpResponse);
    })
  );
  @Effect({ dispatch: false })
  signInRequestLog$ = this.actions$.pipe(
    ofType(UserActionType.signInRequest),
    tap((signInRequest: SignInRequestUser) => {
      console.log('UserEffects@signInRequestLog: ', signInRequest);
    })
  );
  @Effect({ dispatch: false })
  signInResponseLog$ = this.actions$.pipe(
    ofType(UserActionType.signInResponse),
    tap((signInResponse: SignInResponseUser) => {
      console.log('UserEffects@signInResponseLog: ', signInResponse);
    })
  );
  @Effect({ dispatch: false })
  autoSignInRequestLog$ = this.actions$.pipe(
    ofType(UserActionType.autoSignInRequest),
    tap((autoSignInRequest: AutoSignInRequestUser) => {
      console.log('UserEffects@autoSignInRequestLog: ', autoSignInRequest);
    })
  );
  @Effect({ dispatch: false })
  autoSignInResponseLog$ = this.actions$.pipe(
    ofType(UserActionType.autoSignInResponse),
    tap((autoSignInResponse: AutoSignInResponseUser) => {
      console.log('UserEffects@autoSignInResponseLog: ', autoSignInResponse);
    })
  );
  @Effect({ dispatch: false })
  signOutRequestLog$ = this.actions$.pipe(
    ofType(UserActionType.signOutRequest),
    tap((signOutRequest: SignOutRequestUser) => {
      console.log('UserEffects@signOutRequestLog: ', signOutRequest);
    })
  );
  @Effect({ dispatch: false })
  signOutResponseLog$ = this.actions$.pipe(
    ofType(UserActionType.signOutResponse),
    tap((signOutResponse: SignOutResponseUser) => {
      console.log('UserEffects@signOutResponseLog: ', signOutResponse);
    })
  );
  @Effect({ dispatch: false })
  getProfilRequestLog$ = this.actions$.pipe(
    ofType(UserActionType.getProfilRequest),
    tap((getProfilRequest: GetProfilRequestUser) => {
      console.log('UserEffects@getProfilRequestLog: ', getProfilRequest);
    })
  );
  @Effect({ dispatch: false })
  getProfilResponseLog$ = this.actions$.pipe(
    ofType(UserActionType.getProfilResponse),
    tap((getProfilResponse: GetProfilResponseUser) => {
      console.log('UserEffects@getProfilResponseLog: ', getProfilResponse);
    })
  );
  @Effect({ dispatch: false })
  updateProfilRequestLog$ = this.actions$.pipe(
    ofType(UserActionType.updateProfilRequest),
    tap((updateProfilRequest: UpdateProfilRequestUser) => {
      console.log('UserEffects@updateProfilRequestLog: ', updateProfilRequest);
    })
  );
  @Effect({ dispatch: false })
  updateProfilResponseLog$ = this.actions$.pipe(
    ofType(UserActionType.updateProfilResponse),
    tap((updateProfilResponse: UpdateProfilResponseUser) => {
      console.log('UserEffects@updateProfilResponseLog: ', updateProfilResponse);
    })
  );

  @Effect({ dispatch: true })
  signUpRequestSE$ = this.actions$.pipe(
    ofType(UserActionType.signUpRequest),
    switchMap((signUpRequest: SignUpRequestUser) => this.user.signUp(signUpRequest.payload.profil).pipe(
      map((profil: UserProfil) => new SignUpResponseUser({ profil }, signUpRequest.correlationId)),
      catchError((error: Error) => Observable.of(new SetErrorApp({ error }, signUpRequest.correlationId))),
    )),
  );
  @Effect({ dispatch: true })
  signInRequestSE$ = this.actions$.pipe(
    ofType(UserActionType.signInRequest),
    switchMap((signInRequest: SignInRequestUser) => this.user.signIn(signInRequest.payload.credentials).pipe(
      map((response: { profil: UserProfil, token: UserToken }) => new SignInResponseUser(response, signInRequest.correlationId)),
      catchError((error: Error) => Observable.of(new SetErrorApp({ error }, signInRequest.correlationId))),
    )),
  );
  @Effect({ dispatch: true })
  autoSignInRequestSE$ = this.actions$.pipe(
    ofType(UserActionType.autoSignInRequest),
    switchMap((autoSignInRequest: AutoSignInRequestUser) => this.user.signIn(autoSignInRequest.payload.credentials).pipe(
      map((response: { profil: UserProfil, token: UserToken }) => new AutoSignInResponseUser(response, autoSignInRequest.correlationId)),
      catchError((error: Error) => Observable.of(new SetErrorApp({ error }, autoSignInRequest.correlationId))),
    )),
  );
}