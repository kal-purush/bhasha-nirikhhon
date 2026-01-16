using System.Diagnostics;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using Geolocation; // https://github.com/scottschluer/geolocation

namespace Models
{
    public class Organization
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public ObjectId Id { get; set; }
        [BsonElement("Name")]
        public string Name { get; set; }
        [BsonElement("Password")]
        public string Password { get; set; }
        [BsonElement("Place")] 
        public Coordinate Place { get; set; }
        [BsonElement("Contact")] 
        public string Contact { get; set; }
        [BsonElement("Destription")] 
        public string Destription { get; set; }
        [BsonElement("Category")] 
        public int Category { get; set; }
    }
}