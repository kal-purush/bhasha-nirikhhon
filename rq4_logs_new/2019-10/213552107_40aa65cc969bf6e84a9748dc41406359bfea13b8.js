import {ADD_PRODUCT_TO_FREEZER, UPDATE_TEMPERATURE} from "../constants/freezer";

export function updateTemperature(temperature) {
    return {
        type: UPDATE_TEMPERATURE,
        payload: temperature
    };
}


export function addProductToFreezer(name, amount) {
    return {
        type: ADD_PRODUCT_TO_FREEZER,
        payload: {
            name: name,
            amount: amount
        }
    };
}