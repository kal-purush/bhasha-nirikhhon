using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataAccessLayer.Logging
{
    public class ErrorLog
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string ID { get; set; }

        [BsonElement("User")]
        public int userId { get; set; }

        [BsonElement("Request")]
        public string request { get; set; }

        [BsonElement("Message")]
        public string message { get; set; }

        [BsonElement("Layer")]
        public string layer { get; set; }

        [BsonElement("TimeStamp")]
        public DateTime timeStamp{get;set;}

    }
}