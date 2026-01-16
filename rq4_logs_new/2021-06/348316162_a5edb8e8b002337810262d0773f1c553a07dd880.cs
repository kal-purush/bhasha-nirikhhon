using EventPlanner.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace EventPlanner.Services
{
    class NotificationService
    {

        private readonly string PATH = Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), @"Data\notifications.json");
        private static NotificationService singleton = null;


        public static NotificationService Singleton()
        {
            return singleton ??= new NotificationService();
        }

        public Notification GetNotificationInfo(int id)
        {
            return GetNotifications().FirstOrDefault(notification => notification.Id == id);
        }

        public List<Notification> GetNotifications()
        {
            List<Notification> notifications = new List<Notification>();
            using (StreamReader reader = new StreamReader(PATH))
            {
                string data = reader.ReadToEnd();
                notifications = JsonConvert.DeserializeObject<List<Notification>>(data);
            }
            return notifications;
        }

        public List<Notification> GetNotificationsUserFrom(int id)
        {
            List<Notification> notifications = GetNotifications();
            notifications.RemoveAll(notification => notification.UserFromId != id);
            return notifications;
        }

        public List<Notification> GetNotificationsUserTo(int id)
        {
            List<Notification> notifications = GetNotifications();
            notifications.RemoveAll(notification => notification.UserToId != id);
            return notifications;
        }

        public List<Notification> GetNotificationsElement(int id)
        {
            List<Notification> notifications = GetNotifications();
            notifications.RemoveAll(notification => notification.ElementId != id);
            return notifications;
        }

        public List<Notification> GetMessages()
        {
            List<Notification> notifications = GetNotifications();
            notifications.RemoveAll(notification => notification.Type != NotificationType.MESSAGE_SENT);
            return notifications;
        }

        public List<Notification> GetRequests()
        {
            List<Notification> notifications = GetNotifications();
            notifications.RemoveAll(notification => notification.Type != NotificationType.EVENT_REQUEST);
            return notifications;
        }

        public List<Notification> GetChanged()
        {
            List<Notification> notifications = GetNotifications();
            notifications.RemoveAll(notification => notification.Type != NotificationType.EVENT_CHANGED);
            return notifications;
        }

        public List<Notification> GetMessagesUserFrom(int id)
        {
            List<Notification> notifications = GetNotificationsUserFrom(id);
            notifications.RemoveAll(notification => notification.Type != NotificationType.MESSAGE_SENT);
            return notifications;
        }

        public List<Notification> GetRequestsUserFrom(int id)
        {
            List<Notification> notifications = GetNotificationsUserFrom(id);
            notifications.RemoveAll(notification => notification.Type != NotificationType.EVENT_REQUEST);
            return notifications;
        }

        public List<Notification> GetChangedUserFrom(int id)
        {
            List<Notification> notifications = GetNotificationsUserFrom(id);
            notifications.RemoveAll(notification => notification.Type != NotificationType.EVENT_CHANGED);
            return notifications;
        }

        public List<Notification> GetMessagesUserTo(int id)
        {
            List<Notification> notifications = GetNotificationsUserTo(id);
            notifications.RemoveAll(notification => notification.Type != NotificationType.MESSAGE_SENT);
            return notifications;
        }

        public List<Notification> GetRequestsUserTo(int id)
        {
            List<Notification> notifications = GetNotificationsUserTo(id);
            notifications.RemoveAll(notification => notification.Type != NotificationType.EVENT_REQUEST);
            return notifications;
        }

        public List<Notification> GetChangedUserTo(int id)
        {
            List<Notification> notifications = GetNotificationsUserTo(id);
            notifications.RemoveAll(notification => notification.Type != NotificationType.EVENT_CHANGED);
            return notifications;
        }

        public List<Notification> Delete(Notification notification)
        {
            List<Notification> notifications = GetNotifications();
            notifications.RemoveAll(el => el.Id == notification.Id);
            save(notifications);
            return notifications;
        }

        public List<Notification> Modify(Notification notification)
        {
            List<Notification> notifications = GetNotifications();
            for (int i = 0; i < notifications.Count(); i++)
            {
                if (notifications[i].Id == notification.Id)
                {
                    notifications[i] = notification;
                }
            }
            save(notifications);
            return notifications;
        }

        public List<Notification> Add(Notification notification)
        {
            List<Notification> notifications = GetNotifications();
            notifications.Add(notification);
            save(notifications);
            return notifications;
        }

        public int GetLastId()
        {
            List<Notification> notifications = GetNotifications();
            int greatest = 1;
            foreach (var notification in notifications)
            {
                if (greatest < notification.Id)
                {
                    greatest = notification.Id;
                }
            }
            return ++greatest;
        }

        public void save(List<Notification> notifications)
        {
            using (StreamWriter writer = new StreamWriter(PATH))
            {
                string data = JsonConvert.SerializeObject(notifications);
                writer.WriteLine(data);
            }
        }
    }
}