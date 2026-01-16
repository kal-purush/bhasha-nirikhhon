import 'rxjs/add/operator/map';
import 'rxjs/add/operator/mergeMap';
import 'rxjs/add/operator/takeUntil';
import 'rxjs/add/observable/fromPromise';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { Action } from '@ngrx/store';
import { Actions, Effect, toPayload } from '@ngrx/effects';
import { of } from 'rxjs/observable/of';
import {
  MEALS_REMOVE,
  MEALS_SUBSCRIBE, MEALS_UNSUBSCRIBE, MEALS_ADD,
  MealsReceived, MEALS_UPDATE
} from '../shared/actions/meals.actions';
import {AngularFireDatabase} from 'angularfire2/database';
import {AuthService} from '../../auth/shared/services/auth.service';
import {Meal} from '../shared/types/meal';
import {MealsService} from '../shared/services/meals.service';

@Injectable()
export class MealsEffects {
  private extract(firebaseObject) {
    // add $key for array
    if (firebaseObject.length) {
      return firebaseObject.map(item => ({...item, '$key': item.$key}));
    }

    // add $key for single item
    return {
      ...firebaseObject,
      $key: firebaseObject.$key
    };
  }

  @Effect() loadMeals$: Observable<Action> = this.actions$.ofType(MEALS_SUBSCRIBE)
    .switchMap(() =>
      this.db.list(`meals/${this.uid}`)
        .takeUntil(this.actions$.ofType(MEALS_UNSUBSCRIBE))
        .map(
          (meals: Meal[]) => new MealsReceived(this.extract(meals))
        )
    );

  @Effect({dispatch: false}) addMeal$ = this.actions$.ofType(MEALS_ADD)
    .map(toPayload)
    .map(payload => payload.meal)
    .switchMap((meal: Meal) => {
      return Observable.fromPromise(
        this.mealsService.addMeal(meal)
      );
    })

  @Effect({dispatch: false}) updateMeal$ = this.actions$.ofType(MEALS_UPDATE)
    .map(toPayload)
    .map(payload => payload.meal)
    .switchMap((meal: Meal) => {
      return Observable.fromPromise(
        this.mealsService.updateMeal(meal)
      );
    })

  @Effect({dispatch: false}) removeMeal$ = this.actions$.ofType(MEALS_REMOVE)
    .map(toPayload)
    .map(payload => payload.meal)
    .mergeMap((meal: Meal) => this.db.list(`meals/${this.uid}`)
      .remove(meal.$key));

  constructor(
    private db: AngularFireDatabase,
    private authService: AuthService,
    private mealsService: MealsService,
    private actions$: Actions,
  ) {}

  get uid() {
    return this.authService.user.uid;
  }
}