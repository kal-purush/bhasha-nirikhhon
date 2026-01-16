import { Injectable } from '@angular/core';
import { Actions, createEffect, ofType } from '@ngrx/effects';

import { catchError, EMPTY, map, retry, switchMap } from 'rxjs';
import { ApiService } from '../../services/api.service';
import { getAllTrips, getAllTripsComplete } from './actions';

@Injectable()
export class TripsEffects {
  getAllTrips$ = createEffect(() =>
    this.actions$.pipe(
      ofType(getAllTrips.type),
      switchMap(() =>
        this.apiService.getTrips().pipe(
          map(trips => getAllTripsComplete({ trips })),
          retry(1),
          catchError(err => {
            console.error(err);
            return EMPTY;
          })
        )
      )
    )
  );

  constructor(
    private actions$: Actions,
    private apiService: ApiService
  ) {}
}