import {
    createNotification,
    formatGraphqlError
} from 'Modules/Notification/factory'
import { NotificationTypes } from 'Modules/Notification/models'

export const authNotificationsFactory = () => {
    const loginNotificationFailed = (err: Error) =>
        createNotification({
            title: formatGraphqlError(err),
            description: '',
            type: NotificationTypes.FAILURE
        })

    const registerNotificationSucceed = () =>
        createNotification({
            title: 'Successfully registered',
            description: 'Now you can login',
            type: NotificationTypes.SUCCEED
        })

    const registerNotificationFailed = (err: Error) =>
        createNotification({
            title: formatGraphqlError(err),
            description: '',
            type: NotificationTypes.FAILURE
        })

    const notAuthenticatedNotification = () =>
        createNotification({
            title: 'Not Authenticated',
            description: '',
            type: NotificationTypes.FAILURE
        })

    return {
        loginNotificationFailed,
        registerNotificationSucceed,
        registerNotificationFailed,
        notAuthenticatedNotification
    }
}