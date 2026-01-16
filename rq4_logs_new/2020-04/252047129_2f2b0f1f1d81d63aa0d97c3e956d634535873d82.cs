using System;
using System.Collections.Generic;
using System.Text;

namespace HostedQueueProcessor.Core
{
#pragma warning disable CA1710 // Identifiers should have correct suffix
    public class QueueCollectionOptions
#pragma warning restore CA1710 // Identifiers should have correct suffix
    {
        public bool CreateIfNotExists { get; set; }
#pragma warning disable CA2227 // Collection properties should be read only
        public Dictionary<string, string> Queues { get; set; }
#pragma warning restore CA2227 // Collection properties should be read only
    }
}