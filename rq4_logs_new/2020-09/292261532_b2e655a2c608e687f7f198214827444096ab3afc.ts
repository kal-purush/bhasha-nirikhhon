import * as ActionTypes from '../constants/actionTypes';
import * as Models from '../models/userModels';

export const getUserInformation = {
  // TODO: add paylod => uid: fireabase auth uid; beacuse, this action user after login.
  start: () => ({
    type: ActionTypes.GET_USER_PROFILE_START as typeof ActionTypes.GET_USER_PROFILE_START
  }),

  success: (userInfromation: Models.LoginUser) => ({
    type: ActionTypes.GET_USER_PROFILE_SUCCESS as typeof ActionTypes.GET_USER_PROFILE_SUCCESS,
    payload: userInfromation
  }),

  failure: () => ({
    type: ActionTypes.GET_USER_PROFILE_FAILURE as typeof ActionTypes.GET_USER_PROFILE_FAILURE
  })
};

