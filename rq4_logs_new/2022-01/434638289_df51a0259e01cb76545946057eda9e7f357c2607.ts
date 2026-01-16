import {API_INGREDIENTS, API_ORDERS} from "../../const/api";
import {checkResponse} from "../../utils/helper";
import IIngredientItem from "../../types/IngredientItem";

export const SET_INGREDIENTS = 'SET_INGREDIENTS';
export const SET_SELECTED_INGREDIENTS = 'SET_SELECTED_INGREDIENTS';
export const SET_ERROR = 'SET_ERROR';
export const SET_ORDER_LOADING = 'SET_ORDER_LOADING';
export const SET_ORDER = 'SET_ORDER';
export const SET_ORDER_ERROR = 'SET_ORDER_ERROR';
export const SET_CURRENT_INGREDIENT = 'SET_CURRENT_INGREDIENT';

export const GET_INGREDIENTS = () => {
    return (dispatch: (arg: { type: string; payload: any; }) => void) => {
        fetch(API_INGREDIENTS)
            .then(checkResponse)
            .then(({ data }) => {
                if (data) {
                    const ingredients = data.reduce((acc: any, item: IIngredientItem) => {
                        if (!acc[item.type]) acc[item.type] = [];
                        acc[item.type].push(item);
                        return acc;
                    }, {});
                    dispatch({ type: SET_INGREDIENTS, payload: ingredients });
                    dispatch({ type: SET_SELECTED_INGREDIENTS, payload: [ingredients.bun[0], ingredients.sauce[0], ingredients.main[0], ingredients.bun[0]] });
                }
            })
            .catch(() => {
                dispatch({ type: SET_ERROR, payload: true });
            });
    }
};
export const CREATE_ORDER = (ingredients: string) => {
    return (dispatch: (arg: { type: string; payload: any; }) => void) => {
        dispatch({ type: 'SET_ORDER_LOADING', payload: true });
        fetch(API_ORDERS, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: ingredients
        })
            .then(checkResponse)
            .then(({ order }) => {
                if (order) dispatch({ type: 'SET_ORDER', payload: order });
            })
            .catch(() => {
                dispatch({ type: 'SET_ORDER_ERROR', payload: true });
            })
            .finally(() => {
                dispatch({ type: 'SET_ORDER_LOADING', payload: false });
            });
    }
}