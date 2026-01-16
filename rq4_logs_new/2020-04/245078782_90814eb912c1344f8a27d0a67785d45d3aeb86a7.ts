import { Model, model, Types } from 'mongoose'

import { IUserModel } from '../models/interface/user'
import { IRecipeModel } from '../models/interface/recipe'

const Recipe: Model<IRecipeModel> = model('Recipe');
const User: Model<IUserModel> = model('User');

const cleanRecipes = (recipeList: IRecipeModel[]) => recipeList.map(recipe => ({
  id: recipe._id,
  title: recipe.title,
  ingredients: recipe.ingredients,
  directions: recipe.directions,
  createdAt: recipe.createdAt
}))

export const findRecipesByUser = (userID: Types.ObjectId) => User.findById(userID)
  .then(user => user?.recipes ?? [])
  .then(usersRecipeList => Recipe.find({ '_id': { $in: usersRecipeList } }))
  .then(recipeList => cleanRecipes(recipeList))

export const findRecipesByList = (recipeIDList: Types.ObjectId[]) => Recipe.find({ '_id': { $in: recipeIDList } }).then(recipeList => cleanRecipes(recipeList))