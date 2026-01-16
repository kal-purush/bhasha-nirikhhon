using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using planit_data.Entities;

namespace planit_data.DTOs
{
    public class CreateNotificationDTO
    {
        public DateTime TimeStamp { get; set; }
        public int CardId { get; set; }
        public int UserId { get; set; }

        public Notification FromDTO()
        {
            return new Notification()
            {
                CreationTime = TimeStamp,
                IsRead = false
            };
        }
    }

    public class ReadNotificationDTO
    {
        public int NotificationId { get; set; }
        public DateTime TimeStamp { get; set; }
        public bool IsRead { get; set; }
        public int CardId { get; set; }
        public int UserId { get; set; }

        public ReadNotificationDTO(Notification notification)
        {
            NotificationId = notification.NotificationId;
            TimeStamp = notification.CreationTime;
            IsRead = notification.IsRead;
            CardId = notification.Card.CardId;
            UserId = notification.User.UserId;
        }
    }
}