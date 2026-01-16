import { actionTypes } from '@/types/actionTypes';
import { ILoginFailureAction, ILoginRequestAction, ILoginSuccessAction, IUser } from '@/types/authActionsTypes';

export const loginRequest = (): ILoginRequestAction => ({
    type: actionTypes.LOGIN_REQUEST,
});

export const loginSuccess = (user: IUser): ILoginSuccessAction => ({
    type: actionTypes.LOGIN_SUCCESS,
    payload: user,
});

export const loginFailure = (error: string): ILoginFailureAction => ({
    type: actionTypes.LOGIN_FAILURE,
    payload: error,
});

export const logoutSuccess = () => ({
    type: actionTypes.LOGOUT_SUCCESS,
});