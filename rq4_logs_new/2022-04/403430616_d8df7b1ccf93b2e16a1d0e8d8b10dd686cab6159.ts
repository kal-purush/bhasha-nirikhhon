import {createSlice, PayloadAction} from "@reduxjs/toolkit"
import {Recipe} from "FirebaseApi/database"

const initialState: Array<Recipe> = []

export const ingredientsSlice = createSlice({
  name: "recipes",
  initialState,
  reducers: {
    addRecipe: (state, action: PayloadAction<Recipe>) => {
      return [...state, action.payload]
    },
    addRecipes: (_, action: PayloadAction<Array<Recipe>>) => {
      return action.payload
    },
  },
})

export const {addRecipe, addRecipes} = ingredientsSlice.actions

export default ingredientsSlice.reducer