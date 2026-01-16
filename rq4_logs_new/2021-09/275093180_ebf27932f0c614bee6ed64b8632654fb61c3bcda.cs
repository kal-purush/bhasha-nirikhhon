using System;
using Newtonsoft.Json;

namespace In.ProjectEKA.HipService.SmsNotification.Model
{
    public class GatewaySmsNotifyRepresentation
    {
        public Guid RequestId { get; }
        private DateTime Timestamp { get; }
        private SmsNotification SmsNotification { get; }

        public GatewaySmsNotifyRepresentation(Guid requestId, DateTime timestamp,
            SmsNotification smsNotification)
        {
            RequestId = requestId;
            Timestamp = timestamp;
            SmsNotification = smsNotification;
        }

    }
}