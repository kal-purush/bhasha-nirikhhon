using System;
using Entities;

namespace NonPersistentQueueManager
{
    internal class NonPersistentQueue : IQueue
    {
        private readonly IQueueMessageFactory _factory;
        public event EventHandler<IQueueMessage> OnReceived;
        public string Name { get; }

        public NonPersistentQueue(string queueName, IQueueMessageFactory factory)
        {
            Name = queueName;
            _factory = factory;
        }

        public void Post(byte[] message)
        {
            var msg = _factory.CreateQueueMessage();
            msg.Body = message;
            msg.Created = DateTime.Now;

            OnReceived?.Invoke(this, msg);
        }
    }
}