import { filter, pluck, switchMap, map } from 'rxjs/operators'
import { combineEpics } from 'redux-observable'
import { isOfType } from 'typesafe-actions'

import { Epic } from 'Config/rootEpic'
import { AuthService } from './service'
import * as actions from './actions'

export const authEpicFactory = (authService: AuthService): Epic => {
    const authEpic: Epic = action$ =>
        action$.pipe(
            filter(isOfType(actions.LOGIN_REQUEST)),
            pluck('payload'),
            switchMap(variables => authService.login(variables)),
            map(data => actions.loginSucceed(data.data.login))
        )

    return combineEpics(authEpic)
}