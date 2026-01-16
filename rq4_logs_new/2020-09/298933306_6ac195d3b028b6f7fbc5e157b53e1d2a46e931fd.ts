import { Epic } from 'redux-observable';
import { AnyAction } from 'redux';
import { RootState } from 'store/reducers';
import { filter, map, switchMap } from 'rxjs/operators';
import { someAction, someEpicAction } from 'store/actions/someActions';
import { timer } from 'rxjs';

export const someEpic: Epic<AnyAction, AnyAction, RootState> = action$ => {
  return action$.pipe(
    filter(someAction.match),
    map(action => action.payload),
    switchMap(payload => {
      return timer(1000).pipe(
        map(() => someEpicAction(payload + 10000))
      )
    })
  )
}