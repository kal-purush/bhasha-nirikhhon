import 'rxjs/add/operator/switchMap';
import { Injectable } from '@angular/core';
import { Effect, Actions } from '@ngrx/effects';
import { Action } from '@ngrx/store';
import { Observable } from 'rxjs/Observable';

import { RecipesService } from '../services/recipes.service';
import * as recipes from '../actions/recipes';

@Injectable()
export class RecipesEffects {
  constructor(
    private actions$: Actions,
    private recipesService: RecipesService
  ) { }

  @Effect()
  loadrecipes$: Observable<Action>= this.actions$
    .ofType(recipes.LOAD_RECIPES)
    .switchMap(() => this.recipesService.getRecipes())
    .map(data => new recipes.LoadRecipesSuccessAction(data));
}