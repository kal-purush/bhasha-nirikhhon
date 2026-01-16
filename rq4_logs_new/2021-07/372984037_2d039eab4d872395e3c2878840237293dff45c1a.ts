import { Action } from 'redux';
import { GetUserDashBoardAction, GetUserDashboard } from '../actions/getusers.action';
import { IUser } from '../../types/user';

// import { IUserState } from './auth.reducer';

export interface IGetUserState {
    users: IUser[] | undefined;

}

const initialState: IGetUserState = {
  users: undefined,
};

export const getusersdashboard = (state = initialState, action: Action) => {
  switch (action.type) {
    case GetUserDashboard.GET_USERS: {
      const { getusers } = (action as GetUserDashBoardAction).payload;
      return {
        ...state,
        ...{ getusers },
      };
    }

    default:
      return state;
  }
};