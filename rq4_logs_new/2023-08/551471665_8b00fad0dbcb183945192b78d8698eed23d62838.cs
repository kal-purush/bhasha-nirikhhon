using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LightBlueFox.Networking
{

    public delegate void MessageQueueActionHandler(MessageStoreHandle storedMessage);
    internal class MessageQueue
    {
        public MessageQueueActionHandler? QueueAction;


        private BlockingCollection<MessageStoreHandle> storedMessages = new();

        private Task? QueueWorkerTask = null;
        private CancellationTokenSource stopTakingMessages = new();
        private void QueueWorker()
        {
            var token = stopTakingMessages.Token;
            while (!token.IsCancellationRequested)
            {
                MessageStoreHandle msg;
                try
                {
                    msg = storedMessages.Take(token);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                QueueAction?.Invoke(msg);

            }
            QueueWorkerTask = null;

        }

        public bool WorkOnQueue
        {
            get
            {
                return QueueWorkerTask != null;
            }
            set
            {
                if (value && QueueWorkerTask == null) QueueWorkerTask = Task.Run(QueueWorker);
                else if(!value && QueueWorkerTask != null) stopTakingMessages.Cancel();
            }
        }

        public MessageQueue(MessageQueueActionHandler queueAction)
        {
            this.QueueAction = queueAction;
        }

        public void Add(MessageStoreHandle message)
        {
            storedMessages.Add(message);
        }
    }
}