using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Queue;
using Microsoft.Extensions.Configuration;

namespace ConsoleQueueProcessor.Core
{
    public class DocumentQueues
    {
        public CloudQueue SourceQueue { get; private set; }
        public CloudQueue ErrorQueue { get; private set; }
        public CloudQueue SuccessQueue { get; private set; }


        public DocumentQueues(IConfiguration configuration)
        {
            var connectionString = configuration.GetValue<string>("AzureWebJobsStorage");

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();

            SourceQueue = GetQueue(queueClient, configuration.GetValue<string>("SourceQueue"));
            ErrorQueue = GetQueue(queueClient, configuration.GetValue<string>("ErrorQueue"));
            SuccessQueue = GetQueue(queueClient, configuration.GetValue<string>("SuccessQueue"));
        }

        private static CloudQueue GetQueue(CloudQueueClient client, string name)
        {
            var queue = client.GetQueueReference(name);
            queue.CreateIfNotExists();
            return queue;
        }

    }
}