import axios from 'axios';
import * as actions from './types';

const logoutRoute = '/api/me/logout';

export const updateAuthStatus = (auth) => ({
    type: actions.AUTH_UPDATE_STATUS,
    payload: auth,
});

export const requestAuthStatus = (socket) => (dispatch) => {
    socket.emit('status');
    dispatch({type: actions.AUTH_GET_STATUS});
};

export const logoutCurrentUser = (socket) => async (dispatch) => {
    try {
        await axios.delete(logoutRoute);
    } catch (err) {
        console.log('Error while logging out', err);
    }
    dispatch({type: actions.AUTH_LOGOUT});
    if (socket) {
        requestAuthStatus(socket)(dispatch);
    }
};