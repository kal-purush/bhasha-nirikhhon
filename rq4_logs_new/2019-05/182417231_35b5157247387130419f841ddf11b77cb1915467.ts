import { Injectable } from '@angular/core';

import { Actions, Effect, ofType } from '@ngrx/effects';
import { EMPTY } from 'rxjs';
import { catchError, map, mergeMap } from 'rxjs/operators';

import * as Recipes from '../reducer/actions/types';
import * as RecipesActions from '../reducer/actions';

import { RecipesService } from '../services/recipes.service';
import { SingleRecipeItem } from '../commons/interfaces/single-recipe-item.interface';

@Injectable()
export class RecipesEffects {

    constructor(
        private _actions$: Actions,
        private _recipesService: RecipesService,
    ) {}

    @Effect()
    loadRecipes$ = this._actions$
        .pipe(
            ofType(Recipes.ActionTypes.AddRecipes),
            mergeMap(() => this._recipesService.getRecipes()
                .pipe(
                    map((recipes: Array<SingleRecipeItem>) => new RecipesActions.AddRecipesSuccess(recipes)),
                    catchError(() => EMPTY),
                )),
        );


}