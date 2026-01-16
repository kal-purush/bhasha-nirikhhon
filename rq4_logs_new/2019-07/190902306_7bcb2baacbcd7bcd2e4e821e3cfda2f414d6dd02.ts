import { filter, pluck, map, delay, takeUntil } from 'rxjs/operators'
import { combineEpics } from 'redux-observable'
import { isOfType } from 'typesafe-actions'

import { Epic } from 'Config/rootEpic'
import * as actions from './actions'

export const notificationEpicFactory = (): Epic => {
    const notificationEpic: Epic = action$ => {
        const closeNotification$ = action$.pipe(
            filter(isOfType(actions.CLOSE_NOTIFICATION))
        )

        return action$.pipe(
            filter(isOfType(actions.SHOW_NOTIFICATION)),
            takeUntil(closeNotification$),
            delay(3000),
            pluck('payload', 'id'),
            map(actions.closeNotification)
        )
    }

    return combineEpics(notificationEpic)
}