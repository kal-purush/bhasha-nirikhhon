import { showLoginModal } from './LoginModalActions';
import { SHOW_LOGIN_MODAL } from './LoginModalTypes';
import { AnyAction } from 'redux';
import React from "react";

export type LoginModalState = {
    showLoginModal: boolean
}

const initialLoginModalState = {
    showLoginModal: false
}

const loginModalState = (
    state: LoginModalState = initialLoginModalState,
    action: AnyAction
) => {
    switch(action.type) {

        case SHOW_LOGIN_MODAL: {
            return{ showLoginModal: action.payload };
        }

        default:
            return state;
    }
}

export default loginModalState;