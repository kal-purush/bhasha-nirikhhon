import { StateObservable, ActionsObservable } from 'redux-observable'
import { Subject, of } from 'rxjs'

import { registerNotificationSucceed } from '../factory'
import { notificationEpicFactory } from '../epic'
import * as actions from '../actions'

describe('Notification epic', () => {
    const mockState = new StateObservable(new Subject(), {})

    const registerNotification = registerNotificationSucceed()

    it('should show and close notification', done => {
        // I'll refactor this test to using rxjs TestScheduler later
        jest.setTimeout(10000)
        const action$ = of(actions.showNotification(registerNotification))

        const notificationEpicFactoryInstance = notificationEpicFactory()

        notificationEpicFactoryInstance(
            new ActionsObservable(action$),
            mockState,
            null
        ).subscribe(res => {
            expect(res).toEqual(
                actions.closeNotification(registerNotification.id)
            )

            done()
        })
    })
})