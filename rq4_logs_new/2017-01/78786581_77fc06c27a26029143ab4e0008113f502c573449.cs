using System;
using ServiceStack.DataAnnotations;

namespace ChatroServer.Entity
{
    public class Message
    {
        [PrimaryKey, AutoIncrement]
        public int Id { get; set; }

        public DateTime TimeStamp { get; set; }

        public User Sender { get; set; }

        /// <summary>
        /// Recipent of the message. 
        /// If not set, the message is considered as broadcast message.
        /// </summary>
        [Default(null)]
        public User Recipient { get; set; }

        public string Content { get; set; }

        public Message(DateTime timeStamp, User sender, string content) : this(timeStamp, sender, null, content)
        {
            // overload without the recipient parameter
        }

        public Message(DateTime timeStamp, User sender, User recipient, string content)
        {
            if (sender == null)
            {
                throw new ArgumentNullException(nameof(sender));
            }
            if (content == null)
            {
                throw new ArgumentNullException(nameof(content));
            }
            this.TimeStamp = timeStamp;
            this.Sender = sender;
            this.Recipient = recipient;
            this.Content = content;
        }
    }
}