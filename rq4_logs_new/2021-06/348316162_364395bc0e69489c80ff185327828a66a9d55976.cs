using System;
using System.Collections.Generic;
using System.Text;

namespace EventPlanner.Models
{
    public class Message
    {
        public string Content { get; private set; }
        public bool IsCurrentUsersMessage { get; private set; }
        public int SenderId { get; private set; }
        public DateTime TimeStamp { get; private set; }
        public Message(string content, int senderId, DateTime timeStamp, bool isCurrentUsersMessage)
        {
            Content = content;
            SenderId = senderId;
            TimeStamp = timeStamp;
            IsCurrentUsersMessage = isCurrentUsersMessage;
        }
    }
}