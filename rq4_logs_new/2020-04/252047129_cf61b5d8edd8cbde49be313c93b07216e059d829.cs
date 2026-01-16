using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Queue;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using System;
using System.Collections.Generic;

namespace HostedQueueProcessor.Core
{
    /// Usage depends upon config section named "QueueCollection".  As many queue definitions as needed can be included inside.  Queue keys can
    /// be arbitrarily named, and are used when referencing the QueueCollection indexer.  Example:
    /// 
    /// "QueueCollection": {
    ///    "SourceQueue": "dockets-to-be-validated",
    ///    "SuccessQueue": "dockets-to-be-added-to-b1",
    ///    "FailureQueue": "dockets-where-validation-failed"
    /// }
    public class QueueCollection
    {
        private readonly Dictionary<string, CloudQueue> _queueDict = new Dictionary<string, CloudQueue>();
        private readonly ILogger<QueueCollection> _logger;


        public QueueCollection(ILogger<QueueCollection> logger, IConfiguration configuration)
        {
            _logger = logger;

            var connectionString = configuration.GetValue<string>("AzureWebJobsStorage");
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            var queueClient = storageAccount.CreateCloudQueueClient();

            var queueCollectionSection = configuration.GetSection(nameof(QueueCollection));

            if (queueCollectionSection == null)
            {
                throw new InvalidOperationException($"When using QueueCollection class for CloudQueue dependency injection, config must contain section named '{nameof(QueueCollection)}'.");
            }

            foreach (var queueSetting in queueCollectionSection.GetChildren())
            {
                var queueKey = queueSetting.Key;
                var queueName = queueSetting.Value;
                var queue = GetQueue(queueClient, queueName);
                _queueDict.Add(queueKey, queue);
            }
        }


        public static CloudQueue GetQueue(CloudQueueClient client, string name)
        {
            var queue = client.GetQueueReference(name);
            queue.CreateIfNotExists();
            return queue;
        }


        public CloudQueue this[string queueKey]
        {
            get
            {
                if (_queueDict.ContainsKey(queueKey)) return _queueDict[queueKey];

                throw new InvalidOperationException($"{nameof(QueueCollection)}:indexer:  Attempt to reference a queue with key '{queueKey}' failed.  No such queue key.  Make sure your configuration has the path {nameof(QueueCollection)}:{queueKey} with a value corresponding the name of the actual Azure {nameof(CloudQueue)}.");
            }
        }
    }
}