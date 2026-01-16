import { PayloadAction, createSlice } from "@reduxjs/toolkit";
import { variables } from "@/helpers/variables";


export interface favoritesState {
    favorites?: number[]
}

const initialState: favoritesState = {
    favorites: JSON.parse(localStorage.getItem(variables.FAVORITES_LOCALSTORAGE)!) || undefined
}

export const favoritesSlice = createSlice({
    name: 'favorites',
    initialState,
    reducers: {
        removeFavorite: (state, { payload: id }: PayloadAction<number>) => {
            let idIndex = state.favorites?.findIndex(i => i === id)
            if (idIndex && idIndex !== -1) {
                state.favorites?.splice(idIndex, 1)
                localStorage.setItem(variables.FAVORITES_LOCALSTORAGE, JSON.stringify(state.favorites));
            }
        },
        addFavorite: (state, { payload: id }: PayloadAction<number>) => {
            state.favorites?.push(id)
            localStorage.setItem(variables.FAVORITES_LOCALSTORAGE, JSON.stringify(state.favorites));
        }
    },
})

export const { actions, reducer } = favoritesSlice