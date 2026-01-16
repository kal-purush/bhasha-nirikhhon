import { variables } from "@/helpers/variables";
import { PayloadAction, createSlice } from "@reduxjs/toolkit";


export interface watchedState {
    watched?: number[]
}

const initialState: watchedState = {
    watched: JSON.parse(localStorage.getItem(variables.WATCHED_LOCALSTORAGE)!) || undefined
}

export const watchedSlice = createSlice({
    name: 'watched',
    initialState,
    reducers: {
        addWatched: (state, { payload: id }: PayloadAction<number>) => {
            const findId = state.watched?.find(i => i === id)
            if(!findId){
                state.watched?.push(id)
            }
            else if(findId !== (state.watched?.length || 0) - 1 ){
                state.watched?.splice(findId, 1)
                state.watched?.push(id)
            }
        }
    },
})

export const { actions, reducer } = watchedSlice