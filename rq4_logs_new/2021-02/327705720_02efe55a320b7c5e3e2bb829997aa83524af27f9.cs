using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BRIM.BackendClassLibrary
{
    public static class NotificationManager
    {
        private static int currentID = 0;
        private static List<Notification> notifications = new List<Notification>();

        public static void AddNotification(string message)
        {
            notifications.Add(new Notification(currentID, message));
            currentID++;
        }

        public static void RemoveNotification(int id)
        {
           notifications.Remove(notifications.Where(n => n.ID == id).First());
        }
    }
}