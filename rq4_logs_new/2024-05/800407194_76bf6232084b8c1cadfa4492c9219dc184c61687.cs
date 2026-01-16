using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace ChessAI
{
    public class Message
    {
        public string TableCode { get; set; }
        public string type { get; set; }
        public string from { get; set; }
        public string to { get; set; }
        public string message { get; set; }
        public DateTime date { get; set; }

        public Message(string TableCode, string type, string from, string to, string message, DateTime date)
        {
            this.TableCode = TableCode;
            this.type = type;
            this.from = from;
            this.to = to;
            this.message = message;
            this.date = date;
        }

        // Serialize the object to JSON
        public string ToJson()
        {
            return JsonSerializer.Serialize(this);
        }

        // Deserialize the JSON to object
        public static Message FromJson(string json)
        {
            return JsonSerializer.Deserialize<Message>(json);
        }
    }
}