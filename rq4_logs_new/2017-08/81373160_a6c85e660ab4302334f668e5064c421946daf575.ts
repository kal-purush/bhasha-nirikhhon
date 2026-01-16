import { Action } from '@ngrx/store';
import { IRecipes } from '../models/recipes';

export const LOAD_RECIPES =  '[Recipes] Load Recipes';
export const LOAD_RECIPES_SUCCESS = '[Recipes] Load Recipes Success';


export class LoadRecipesAction implements Action {
  readonly type = LOAD_RECIPES;

  constructor() { }
}

export class LoadRecipesSuccessAction implements Action {
  readonly type = LOAD_RECIPES_SUCCESS;

  constructor(public payload: IRecipes[]) { }
}

export type Actions
  = LoadRecipesAction
  | LoadRecipesSuccessAction;