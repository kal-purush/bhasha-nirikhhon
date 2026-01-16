using System;
using System.Collections.Generic;
using System.Text;

namespace TaskDudes.Models
{
    public class Settings
    {
        public Settings(NotificationType notificationType)
        {
            NotificationType = notificationType;
        }
        public NotificationType NotificationType { get; set; }
        //public Coach Coach {get; set;} = new Coach();

    }

    public enum NotificationType
    {
        Email,
        PushNotification,
        IconBatch,
        InAppNotificaton,
        None,
        All = Email + PushNotification + IconBatch + InAppNotificaton
    }
}