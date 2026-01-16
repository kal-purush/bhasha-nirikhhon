import { combineReducers } from "@reduxjs/toolkit";

const initialState = []

const reducer = (state = initialState, action) => {
    switch (action.type) {
        case "ADD_DATA": {
            console.log("here0");
            return [...state, action.payload];
        }
        case "DELETE_DATA": {
            console.log("here1");
            return state.filter((item) => item.id !== action.payload);
        }
        case "EDIT_DATA": {
            console.log("here2");
            return state.map((item) => {
                if (item.id === action.payload.id) {
                    return action.payload;
                }
                return item;
            });
        }
        default: {
            return state;
        }
    }
};

const reducers = combineReducers({
    data: reducer,
});

export default reducers;