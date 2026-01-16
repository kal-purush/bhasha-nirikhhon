import { NotificationType } from '../../../../../../src/modules/app/components/notification/notification.model';

const notificationState: any = {
  notification: {
    notifications: [
      {
        message: 'first notification',
        type: NotificationType.error
      },
      {
        message: 'second notification',
        type: NotificationType.error
      },
      {
        message: 'third notification',
        type: NotificationType.success
      }
    ]
  }
};

export default notificationState;