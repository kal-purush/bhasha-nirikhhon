import { Action } from 'redux';

export type Actions =
    SetItem |
    AddItem |
    DeleteItem |
    ModifyQuantity
    ;

export interface SetItem extends Action {
    type: 'SET_ITEM';
    payload: {
        id: string;
    }
}
export function setItem(id: string): SetItem {
    return {
        type: 'SET_ITEM',
        payload: {
            id
        }
    };
}

export interface AddItem extends Action {
    type: 'ADD_ITEM';
}
export function addItem(): AddItem {
    return {
        type: 'ADD_ITEM'
    };
}

export interface DeleteItem extends Action {
    type: 'DELETE_ITEM';
    payload: {
        id: string;
    }
}
export function deleteItem(id: string): DeleteItem {
    return {
        type: 'DELETE_ITEM',
        payload: {
            id
        }
    };
}

export interface ModifyQuantity extends Action {
    type: 'MOD_QUANTITY';
    payload: {
        id: string;
        quantity: number;
    };
}
export function modifyQuantity(id, quantity): ModifyQuantity {
    return {
        type: 'MOD_QUANTITY',
        payload: {
            id,
            quantity
        }
    };
}


