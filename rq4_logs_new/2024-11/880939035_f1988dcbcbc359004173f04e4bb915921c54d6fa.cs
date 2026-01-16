using System;

namespace ClientApp
{
    public class Messaging
    {
        public int Id { get; set; }
        public string Text { get; set; }
        public DateTime Time { get; set; }
        public string Data { get; set; }

        public User User { get; set; }
        public int UserId { get; set; }

        public Messaging(int id,string text, DateTime time, string data = null)
        {
            Id = id;
           Text = text;
            Time = time;
            Data = data;
        }
    }
}
