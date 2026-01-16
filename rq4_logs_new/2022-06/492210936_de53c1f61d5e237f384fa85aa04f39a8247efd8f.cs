using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Simsprojekat.Model
{

    [BsonIgnoreExtraElements]
    public class Adress
    {
        [BsonElement("streetName")]
        public string StreetName { get; set; }

        [BsonElement("streetNumber")]
        public string StreetNumber { get; set; }

        [BsonElement("city")]
        public City City { get; set; }

    }
}