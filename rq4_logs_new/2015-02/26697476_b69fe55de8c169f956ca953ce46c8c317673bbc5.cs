using System;

namespace FusionMVVM.Service
{
    public class EventAggregator : IEventAggregator
    {
        /// <summary>
        /// Subscribes the target to receive messages of type TMessage.
        /// </summary>
        /// <typeparam name="TMessage"></typeparam>
        /// <param name="target"></param>
        /// <param name="action"></param>
        public void Subscribe<TMessage>(object target, Action<TMessage> action)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Unsubscribes the message of type TMessage from the target.
        /// </summary>
        /// <typeparam name="TMessage"></typeparam>
        /// <param name="target"></param>
        public void Unsubscribe<TMessage>(object target)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Publishes a message of type TMessage to all subscribers.
        /// </summary>
        /// <typeparam name="TMessage"></typeparam>
        /// <param name="message"></param>
        public void Publish<TMessage>(TMessage message)
        {
            throw new NotImplementedException();
        }
    }
}