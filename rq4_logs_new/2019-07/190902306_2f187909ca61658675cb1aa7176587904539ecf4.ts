import {
    CreateNotification,
    formatGraphqlError
} from 'Modules/Notification/factory'
import { NotificationTypes } from 'Modules/Notification/models'

export type CompanyNotificationsFactory = ReturnType<
    typeof companyNotificationsFactory
>

export const companyNotificationsFactory = (
    createNotification: CreateNotification
) => {
    const createCompanySuccess = () =>
        createNotification({
            title: 'Successfully created company',
            description: '',
            type: NotificationTypes.SUCCEED
        })

    const createCompanyFailed = (err: Error) =>
        createNotification({
            title: formatGraphqlError(err),
            description: '',
            type: NotificationTypes.FAILURE
        })

    return {
        createCompanySuccess,
        createCompanyFailed
    } as const
}