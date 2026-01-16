import { Action } from 'redux';
import { GetDashboardUsersAction, DashboardActions } from '../actions/dashboard.action';
import { IUser } from '../../types/user';

// import { IUserState } from './auth.reducer';

export interface IDashboard {
    users: IUser[] | undefined;

}

const initialState: IDashboard = {
  users: undefined,
};

export const dashboard = (state = initialState, action: Action): IDashboard => {
  switch (action.type) {
    case DashboardActions.GET_USERS: {
      const { users } = (action as GetDashboardUsersAction).payload;
      return {
        ...state,
        ...{ users },
      };
    }

    default:
      return state;
  }
};