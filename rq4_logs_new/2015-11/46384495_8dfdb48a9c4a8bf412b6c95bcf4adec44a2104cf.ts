import { ACTIONS } from '../constants'

export function increment() {
    return {
        type: ACTIONS.INCREMENT_COUNTER
    }
};

export function decrement() {
    return {
        type: ACTIONS.DECREMENT_COUNTER
    };
};

export function incrementIfOdd() {
    return (dispatch, getState) => {
        const { counter } = getState();

        if (counter % 2 === 0) {
            return
        }

        dispatch(increment());
    };
};

export function incrementAsync(delay = 1000) {
    return dispatch => {
        setTimeout(() => {
            dispatch(increment())
        }, delay);
    };
};