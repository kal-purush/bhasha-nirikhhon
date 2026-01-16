using System;

namespace Entities
{
    public interface IActionable
    {
        string Error { get; }
        Action NextStep { get; }
    }

    public interface IQueueManager
    {
        IQueue CreateQueue(string queuename);

        void Subscribe(string queuename, Func<IQueueMessage, IActionable> callback);
    }

    public interface IQueueMessage
    {
        DateTime Created { get; set; }
        byte[] Body { get; set; }
    }

    public interface IQueue
    {
        event EventHandler<IQueueMessage> OnReceived;
        string Name { get; }
        void Post(byte[] message);
    }

    public interface IQueueFactory
    {
        IQueue CreateQueue(string name);
    }

    public interface IQueueMessageFactory
    {
        IQueueMessage CreateQueueMessage();
    }

}