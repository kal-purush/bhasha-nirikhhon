import { createStore } from 'redux';

const initialState = {
    UsersData: [],
};

export const store = createStore(stateReducer, initialState)


function stateReducer(state = initialState, action) {
    switch (action.type) {
        default:
            return state;
    }
}


export function dispatchAddUser(data) {
    return store.dispatch({
        type: 'ADD_USER',
        text: data
    });
}

export function dispatchDeleteUser(data) {
    return store.dispatch({
        type: 'DELETE_USER',
        text: data
    });
}

export function getCurrentStore() {
    return store.getState();
}

export default {
    dispatchAddUser,
    dispatchDeleteUser,
    getCurrentStore
};

//https://medium.com/@jrcreencia/persisting-redux-state-to-local-storage-f81eb0b90e7e