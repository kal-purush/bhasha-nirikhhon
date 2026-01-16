import axios from 'axios';
import { browserHistory } from 'react-router';
import { AUTH_USER, AUTH_ERROR, NOTAUTH_USER, FETCH_MESSAGE } from './types';

const ROOT_URL = 'http://localhost:3090';

export function signinUser({ email, password }) {
    return function(dispatch) {
        // submit email/pw to server
        axios.post(`${ROOT_URL}/signin`, { email, password })
            // if request is good
            .then(response => {
                // - update state to indicate user is auth'd
                dispatch({ type: AUTH_USER });
            
                // - save the JWT token
                localStorage.setItem('token', response.data.token);
            
                // - redirect to the route '/feature'
                browserHistory.push('/feature');
            })
            // if request is bad
            .catch(() => {
                // - show an error to the user
                dispatch(authError('Bad login info'));
            });
    }
}

export function signupUser({ email, password }) {
    return function(dispatch) {
        axios.post(`${ROOT_URL}/signup`, { email, password })
            .then(response => {
                dispatch({ type: AUTH_USER });
                localStorage.setItem('token', response.data.token);
                browserHistory.push('/feature');
            })
            .catch(response => { dispatch(authError(response.data.error));
            });
    }
}


export function authError(error) {
    return {
        type: AUTH_ERROR,
        payload: error
    }
}

export function signoutUser() {
    localStorage.removeItem('token');
    return { type: NOTAUTH_USER };
}

export function fetchMessage() {
    return function(dispatch) {
        axios.get(ROOT_URL, {
            headers: { authorization: localStorage.getItem('token') }
        })
            .then(response => {
                dispatch({
                    type: FETCH_MESSAGE,
                    payload: response.data.message
                })
            });
    }
}