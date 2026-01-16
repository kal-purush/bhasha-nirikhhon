import { Request, Response } from 'express';
import { Model, model, Types } from 'mongoose';
import { decode } from 'jsonwebtoken'

import { IRecipeModel } from '../models/interface/recipe';

const Recipe: Model<IRecipeModel> = model('Recipe');

export const createRecipe = (req: Request, res: Response) => {
  const { body, cookies } = req;
  const { token } = cookies;
  const { title, ingredients, directions }: { title: string, ingredients: string[], directions: string[] } = body;

  const decodedToken = decode(token);
  // @ts-ignore
  const { id: userID }: { userID: Types.ObjectId } = decodedToken



  if (!title) return res.status(400).json({ errors: { title: 'is required' } })
  if (!ingredients) return res.status(400).json({ errors: { ingredients: 'is required' } })
  if (!directions) return res.status(400).json({ errors: { directions: 'is required' } })

  const recipe = new Recipe({ title, ingredients, directions })

  recipe.save()
    .then(() => res.sendStatus(200))
    .catch(err => {
      console.log('recipe -> createRecipe error:', err);

      return res.status(500).json({ errors: { err } })
    })
}