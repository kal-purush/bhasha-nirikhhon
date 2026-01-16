import {createSlice, PayloadAction} from "@reduxjs/toolkit"
import {Ingredient} from "FirebaseApi/database"

const initialState: Array<Ingredient> = []

export const ingredientsSlice = createSlice({
  name: "ingredients",
  initialState,
  reducers: {
    addIngredient: (state, action: PayloadAction<Ingredient>) => {
      state = [...state, action.payload]
    },
    addIngredients: (_, action: PayloadAction<Array<Ingredient>>) => {
      return action.payload
    },
  },
})

export const {addIngredient, addIngredients} = ingredientsSlice.actions

export default ingredientsSlice.reducer