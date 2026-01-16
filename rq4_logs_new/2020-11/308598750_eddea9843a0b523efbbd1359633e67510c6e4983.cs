using System;

namespace api.layer
{
    public class RatingEntity
    {
        public int PRId { get; set; }

        public long UserId { get; set; }

        public string UserName { get; set; }

        public int RatingPoints { get; set; }

        public DateTime PRUpdatedTime { get; set; }
    }
}