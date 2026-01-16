import {createAsyncThunk} from "@reduxjs/toolkit";
import {IngredientModel} from "../../models/burger-data.model";

let _baseUrl = 'https://norma.nomoreparties.space/api/'

const fetchIngredientsFromAPI = async (): Promise<any> => {
    const response = await fetch(`${_baseUrl}ingredients`);
    if (!response.ok) {
        throw new Error('Не удалось загрузить данные');
    }
    return await response.json();
};

export const fetchIngredients = createAsyncThunk<IngredientModel[]>(
    'ingredients/fetchIngredients',
    async () => {
        const response = await fetchIngredientsFromAPI();
        return response.data;
    }
);