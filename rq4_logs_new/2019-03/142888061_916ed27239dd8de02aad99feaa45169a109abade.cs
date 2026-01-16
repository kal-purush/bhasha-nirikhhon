using System;

namespace NCS.DSS.ContentPushService.Models
{
    public class NotificationDC
    {
        public string TouchpointId { get; set; }
        public Uri ResourceURL { get; set; }
        public DateTime LastModifiedDate { get; set; }
    }
}