using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dynastio.Data
{
    public class Meme
    {
        [BsonId]
        public ObjectId Id { get; set; }
        public string MessageLink { get; set; }
        public string PictureLink { get; set; }

        public ulong Author { get; set; }
        public DateTime CreateAt { get; set; }
        public string Locale { get; set; } = "en-US";
        [BsonIgnore] public int Count { get; set; } = 0;
        public List<ulong> Likes { get; set; } = new();
        public bool Like(ulong id)
        {
            if (Likes.Contains(id)) return false;
            Likes.Add(id);
            return true;
        }
    }

}