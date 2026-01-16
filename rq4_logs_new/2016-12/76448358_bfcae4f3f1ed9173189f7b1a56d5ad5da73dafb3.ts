import * as actionTypes from "@app/constants/actionTypes";
import { RequestLogin, ResponseError, ResponseAuth } from "@app/models/api";

const requestLogin =  actionTypes.LOGIN_REQUEST;
const loginSuccess =  actionTypes.LOGIN_SUCCESS;
const loginFailure =  actionTypes.LOGIN_FAILURE;

interface Action<S> {
    type: string;
    payload?: S;
}

type AuthAction = Action<RequestLogin> | Action<ResponseAuth> | Action<ResponseError>;

export {
    requestLogin,
    loginSuccess,
    loginFailure,
    AuthAction,
    Action
};