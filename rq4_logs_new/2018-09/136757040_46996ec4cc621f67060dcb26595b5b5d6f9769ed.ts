import { ActionReducer } from '@ngrx/store';

import { AuthStorageService } from './auth-storage.service';
import { AuthAction } from '../actions';
import { ReduxAction } from './redux-action';

const storage = new AuthStorageService();

export interface AuthInfo {
  userName: string;
  token: string;
}


export const authReducer: ActionReducer<AuthInfo, ReduxAction<AuthInfo>> = (state = storage.getInfo(), action) => {
  switch (action.type) {
    case AuthAction.logined:
      storage.setInfo(action.payload.userName, action.payload.token);
      return action.payload;

    case AuthAction.logouted:
      storage.cleanUp();
      return undefined;
    default:
      return state;
  }
};