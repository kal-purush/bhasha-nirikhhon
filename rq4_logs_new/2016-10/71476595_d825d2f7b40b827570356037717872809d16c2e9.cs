using Prism.Events;
using Prism.Logging;
using System.ComponentModel.Composition;

namespace TplPlayground.Core.Logging
{
    public class EventPublishingLogger : ILoggerFacade
    {
        private readonly ILoggerFacade _wrappedLogger;

        [Import(typeof(IEventAggregator))]
        private IEventAggregator EventAggregator { get; set; }

        public EventPublishingLogger(ILoggerFacade wrappedLoggerFacade)
        {
            this._wrappedLogger = wrappedLoggerFacade;
        }

        public void Log(string message, Category category, Priority priority)
        {
            this._wrappedLogger.Log(message, category, priority);

            // TODO: Publish event
            if (EventAggregator != null)
            {
                EventAggregator.GetEvent<LogMessageEvent>().Publish(new LogMessage(message, category, priority));
            }
        }
    }
}