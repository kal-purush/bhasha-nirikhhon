import firebase from "./index"

import {getDatabase} from "firebase/database"

export const DB = getDatabase(firebase)

type FoodGroup =
  | "fruit/veg"
  | "meat/poultry"
  | "seafood/fish"
  | "dairy"
  | "herb"
  | "spice"
  | "seasoning"
  | "tin/jar"
  | "sauce"
  | "cupboard"

type FoodUnit = "gram" | "kg" | "ml" | "ltr" | "cup" | "each"

interface Ingredient {
  id: string
  name: string
  unit: FoodUnit
  group: FoodGroup
  unitInGrams?: number
  variants?: Array<string>
}

export const ingredients: Record<string, Ingredient> = {
  abc: {
    id: "abc",
    name: "Pepper",
    unit: "each",
    group: "fruit/veg",
    unitInGrams: 150,
    variants: ["Green", "Red", "Yellow", "Orange"],
  },
  def: {
    id: "def",
    name: "Pasta",
    unit: "gram",
    group: "cupboard",
    variants: ["Spaghetti", "Farfalle", "Penne"],
  },
  bcd: {
    id: "bcd",
    name: "Onions",
    unit: "each",
    group: "fruit/veg",
    variants: ["Red", "Brown", "Shallots"],
  },
  cde: {
    id: "cde",
    name: "Mince",
    unit: "kg",
    unitInGrams: 1000,
    group: "meat/poultry",
    variants: ["Beef", "Pork", "Lamb", "Turkey"],
  },
  efg: {
    id: "efg",
    name: "Passata",
    unit: "ml",
    group: "cupboard",
  },
  fgh: {
    id: "fgh",
    name: "Oregano",
    unit: "gram",
    group: "herb",
  },
}

interface RecipeStep {
  title: string
  directions?: string
}

interface RecipeIngredient {
  id: string
  quantity: number
  note?: string
  variant?: string
}

interface Recipe {
  name: string
  ingredients: Array<RecipeIngredient>
  description?: string
  serves?: number
  cookingInstructions?: Array<RecipeStep>
}

export const recipes: Record<string, Recipe> = {
  bcd: {
    name: "Spag Bowl",
    ingredients: [
      {id: "abc", quantity: 1, note: "Chopped", variant: "Red"},
      {id: "def", quantity: 150, variant: "Spaghetti"},
      {id: "bcd", quantity: 1, variant: "Brown"},
      {id: "cde", quantity: 0.5, variant: "Beef"},
      {id: "efg", quantity: 400},
      {id: "fgh", quantity: 5},
    ],
    serves: 4,
    description: "Meat and pasta",
    cookingInstructions: [
      {title: "Fill pan", directions: "Put in pasta and boil with water"},
    ],
  },
}