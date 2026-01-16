import { createStore } from 'redux'

const initialState = {

    imgData: "",
}

const store = createStore(stateReducer, initialState)


function stateReducer(state = initialState, action) {
    switch (action.type) {
        case 'ADD_IMAGE':
            return state.imgData = action.text

        default:
            return state
    }
}

export function dispatchAddImage(data) {
    return store.dispatch({
        type: 'ADD_IMAGE', text:
            data
    })
}

function getCurrentStore(){
console.log(store.getState())
}