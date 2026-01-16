import { CreateNotification } from 'Modules/Notification/factory'
import { NotificationTypes } from 'Modules/Notification/models'

export type CastingNotificationsFactory = ReturnType<
    typeof castingNotificationsFactory
>

export const castingNotificationsFactory = (
    createNotification: CreateNotification
) => {
    const createCastingNotificationSuccess = () =>
        createNotification({
            title: 'Successfully created casting',
            description: '',
            type: NotificationTypes.SUCCEED
        })

    const createCastingNotificationFailed = () =>
        createNotification({
            title: 'Something went wrong',
            description: '',
            type: NotificationTypes.FAILURE
        })

    return {
        createCastingNotificationSuccess,
        createCastingNotificationFailed
    } as const
}