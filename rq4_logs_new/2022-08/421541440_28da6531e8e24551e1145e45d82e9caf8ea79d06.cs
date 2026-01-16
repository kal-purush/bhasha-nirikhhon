using System;
using System.Collections.Generic;
using System.Text;

namespace Hospital.Events.Model
{
    public class Event
    {
        public int Id { get; set; }
        public DateTime Timestamp { get; set; }
        public String Content { get; set; }
        public int UserId { get; set; }

        public Event()
        {
        }

        public Event(int id, string content, int userId)
        {
            Id = id;
            Timestamp = DateTime.Now;
            Content = content;
            UserId = userId;
        }

    }
}