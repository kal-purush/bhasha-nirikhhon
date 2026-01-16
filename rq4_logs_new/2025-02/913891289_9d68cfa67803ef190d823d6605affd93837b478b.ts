import { createSlice, type PayloadAction } from "@reduxjs/toolkit"
import type { RootState } from "./store"

interface NotesState {
    [productId: string]: string
}

const initialState: NotesState = {}

const notesSlice = createSlice({
    name: "notes",
    initialState,
    reducers: {
        setNote: (state, action: PayloadAction<{ productId: string; note: string }>) => {
            const { productId, note } = action.payload
            state[productId] = note
        },
        clearNote: (state, action: PayloadAction<string>) => {
            delete state[action.payload]
        },
        clearAllNotes: (state) => {
            return {}
        },
    },
})

export const { setNote, clearNote, clearAllNotes } = notesSlice.actions

export const selectNote = (state: RootState, productId: string) => state.notes[productId] || ""

export default notesSlice.reducer
