import { Action, Dispatch } from "redux";
import {
  changeDataAboutUser,
  changePasswordPostData,
  registrationPostData,
  signInPostData,
} from "@/api/signInRegistrationQuery";

import {
  CHANGE_DATA_ABOUT_USER_CONFIRMED_ACTION,
  CHANGE_DATA_ABOUT_USER_FAILED_ACTION,
  CHANGE_PASSWORD_CONFIRMED_ACTION,
  CHANGE_PASSWORD_FAILED_ACTION,
  LOG_OUT_ACTION,
  REGISTER_CLICK_ACTION,
  REGISTRATION_CONFIRMED_ACTION,
  REGISTRATION_FAILED_ACTION,
  SET_MODAL_ACTIVE,
  SET_MODAL_IN_ACTIVE,
  SIGN_IN_CONFIRMED_ACTION,
  SIGN_IN_FAILED_ACTION,
  SIGN_IN_MODAL_ACTIVE,
  SIGN_IN_PARAMS_ACTION,
} from "@/store/modules/auth/auth.constants";

export function signInConfirmedAction(data: object) {
  return {
    type: SIGN_IN_CONFIRMED_ACTION,
    payload: data,
  };
}
export function signInFailedAction(message: string) {
  return {
    type: SIGN_IN_FAILED_ACTION,
    payload: message,
  };
}
export function registrationConfirmedAction(data: object) {
  return {
    type: REGISTRATION_CONFIRMED_ACTION,
    payload: data,
  };
}
export function registraionFailedAction(message: string) {
  return {
    type: REGISTRATION_FAILED_ACTION,
    payload: message,
  };
}

export function changePasswordConfirmedAction(data: object) {
  return {
    type: CHANGE_PASSWORD_CONFIRMED_ACTION,
    payload: data,
  };
}

export function changePasswordFailedAction(message: string) {
  return {
    type: CHANGE_PASSWORD_FAILED_ACTION,
    payload: message,
  };
}

export function logOutAction() {
  return {
    type: LOG_OUT_ACTION,
  };
}
export function signInAction(formData: object) {
  return (dispatch: Dispatch<Action>) => {
    signInPostData({ formData })
      .then((res) => {
        if (res.data) {
          console.log(res.data);
          dispatch(signInConfirmedAction(res.data));
        } else {
          throw Error();
        }
      })
      .catch((err) => {
        dispatch(signInFailedAction(err));
      });
  };
}

export function registrationAction(formData: object) {
  return (dispatch: Dispatch<Action>) => {
    registrationPostData({ formData })
      .then((res) => {
        if (res.data) {
          dispatch(registrationConfirmedAction(res.data));
        } else {
          throw Error();
        }
      })
      .catch((err) => {
        dispatch(registraionFailedAction(err));
      });
  };
}

export function changePasswordAction(formData: object) {
  return (dispatch: Dispatch<Action>) => {
    changePasswordPostData({ formData })
      .then((res) => {
        if (res.data) {
          dispatch(changePasswordConfirmedAction(res.data));
        } else {
          throw Error();
        }
      })
      .catch((err) => {
        dispatch(changePasswordFailedAction(err));
      });
  };
}

export function setModalActive() {
  return {
    type: SET_MODAL_ACTIVE,
  };
}
export function setModalInActive() {
  return {
    type: SET_MODAL_IN_ACTIVE,
  };
}
export function signInModalActive() {
  return {
    type: SIGN_IN_MODAL_ACTIVE,
  };
}
export function signInParamsAction() {
  return {
    type: SIGN_IN_PARAMS_ACTION,
  };
}
export function registerClickAction() {
  return {
    type: REGISTER_CLICK_ACTION,
  };
}
export function changeDataAboutUserConfirmedAction(data: object) {
  return {
    type: CHANGE_DATA_ABOUT_USER_CONFIRMED_ACTION,
    payload: data,
  };
}
export function changeDataAboutUserFailedAction(message: string) {
  return {
    type: CHANGE_DATA_ABOUT_USER_FAILED_ACTION,
    payload: message,
  };
}
export function changeDataAboutUserAction(formData: object) {
  return (dispatch: Dispatch<Action>) => {
    changeDataAboutUser({ formData })
      .then((res) => {
        if (res.data) {
          dispatch(changeDataAboutUserConfirmedAction(res.data));
        } else {
          throw Error();
        }
      })
      .catch((err) => {
        dispatch(changeDataAboutUserFailedAction(err));
      });
  };
}