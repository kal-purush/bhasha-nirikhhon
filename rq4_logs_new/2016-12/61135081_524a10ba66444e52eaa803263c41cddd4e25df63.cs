using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.ServiceBus.Messaging;
using Ve.Messaging.Model;

namespace Ve.Messaging.Azure.ServiceBus.Consumer
{
    public class TransactionalMessageConsumer : MessageConsumer
    {
        public TransactionalMessageConsumer(SubscriptionClient client) : base(client) { }

        protected override IEnumerable<Message> ReceiveMessages(int messageAmount, TimeSpan tm)
        {
            return _client.ReceiveBatch(messageAmount, tm)
                .Where(x => x.State == MessageState.Active)
                .Select(x => new Message(x.GetBody<Stream>(), x.SessionId, x.Label, x.MessageId, x.Properties, x.Complete));
        }
    }
}