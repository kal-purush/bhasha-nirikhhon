using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DataBus.Interfaces;
using MassTransit;

namespace DataBus
{
    public class DatabusExecutionContext
    {
        static DatabusExecutionContext()
        {
            Current = new RabbitExecutionContext();
        }

        public static void SetExecutionContext(IExecutionContext executionContext)
        {
            Current = executionContext;
        }

        public static IExecutionContext Current { get; private set; }
    }

    public class RabbitExecutionContext: IExecutionContext
    {
        private Dictionary<Type, HandlerInfo> _messageHandlersDictionary;

        public void SetHandlers(Dictionary<Type, HandlerInfo> messageHandlersDictionary)
        {
            _messageHandlersDictionary = messageHandlersDictionary;
        }

        public Task Handler<TMessage>(ConsumeContext<TMessage> context) where TMessage : class
        {
            var messageHandler = (Interfaces.MessageHandler<TMessage>)Activator.CreateInstance(_messageHandlersDictionary[typeof(TMessage)].MessageHandlerType);
            return messageHandler.MessageHandle(context.Message);
        }
    }
}