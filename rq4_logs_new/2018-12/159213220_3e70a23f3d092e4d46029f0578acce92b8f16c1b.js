export const ADD_TO_BASKET = 'ADD_TO_BASKET';
export const DELETE_FROM_BASKET = 'DELETE_FROM_BASKET';
export const UPDATE_ITEM_UNITS = 'UPDATE_ITEM_UNITS';

export function updateItemUnits({ id, units }) {
    return {
        type: UPDATE_ITEM_UNITS,
        payload: { id, units }
    }
}

export function addToBasket({ id, name, description, price, units = 1 }) {
    return {
        type: ADD_TO_BASKET,
        payload: {id, name, description, price, units}
    }
}

export function deleteFromBasket({id}) {
    return {
        type: DELETE_FROM_BASKET,
        payload: {id}
    }
}