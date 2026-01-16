using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Queue;

namespace ConsoleQueueProcessor.Core
{
    public class DocumentMessageQueue
    {
        private readonly CloudQueue _cloudQueue;

        public static DocumentMessageQueue CreateQueue(string connectionString, string queueName)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            CloudQueue queue = queueClient.GetQueueReference(queueName);

            // Create the queue if it doesn't already exist
            queue.CreateIfNotExists();

            return new DocumentMessageQueue(queue);
        }

        private DocumentMessageQueue(CloudQueue cloudQueue)
        {
            _cloudQueue = cloudQueue;
        }

        //
        // Summary:
        //     Initiates an asynchronous operation to add a message to the queue.
        //
        // Parameters:
        //   message:
        //     A Microsoft.Azure.Storage.Queue.CloudQueueMessage object.
        //
        // Returns:
        //     A System.Threading.Tasks.Task object that represents the asynchronous operation.
        //
        // Remarks:
        //     The Microsoft.Azure.Storage.Queue.CloudQueueMessage message passed in will be
        //     populated with the pop receipt, message ID, and the insertion/expiration time.
        [DoesServiceRequest]
        public virtual Task AddMessageAsync(Guid messageId)
        {
            return AddMessageAsync(messageId, default);
        }

        //
        // Summary:
        //     Initiates an asynchronous operation to add a message to the queue.
        //
        // Parameters:
        //   message:
        //     A Microsoft.Azure.Storage.Queue.CloudQueueMessage object.
        //
        //   cancellationToken:
        //     A System.Threading.CancellationToken to observe while waiting for a task to complete.
        //
        // Returns:
        //     A System.Threading.Tasks.Task object that represents the asynchronous operation.
        //
        // Remarks:
        //     The Microsoft.Azure.Storage.Queue.CloudQueueMessage message passed in will be
        //     populated with the pop receipt, message ID, and the insertion/expiration time.
        //[DoesServiceRequest]
        public virtual Task AddMessageAsync(Guid messageId, CancellationToken cancellationToken)
        {
            CloudQueueMessage message = new CloudQueueMessage(messageId.ToString());
            return _cloudQueue.AddMessageAsync(message, cancellationToken);
        }
    }
}