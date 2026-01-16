using System;
using System.Collections.Generic;

namespace exercise1.models
{
    public class Campaign
    {
        public string Id { get; set; }
        public int WebId { get; set; }
        public string Type { get; set; }
        public DateTime CreateTime { get; set; }
        public string ArchiveUrl { get; set; }
        public string LongArchiveUrl { get; set; }
        public string Status { get; set; }
        public int EmailsSent { get; set; }
        public string SendTime { get; set; }
        public string ContentType { get; set; }
        public bool NeedsBlockRefresh { get; set; }
        public Recipients Recipients { get; set; }
        public Settings Settings { get; set; }
        public Tracking Tracking { get; set; }
        public DeliveryStatus DeliveryStatus { get; set; }
        public List<Link> Links { get; set; }
    }

}