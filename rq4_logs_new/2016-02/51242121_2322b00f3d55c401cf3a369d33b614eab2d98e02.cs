using System;
using System.Diagnostics.Tracing;

namespace ItAcademy.PropertyCenter.Core.Logging
{
    public class DomainEventSource : EventSource
    {
        private static readonly Lazy<DomainEventSource> Instance =
            new Lazy<DomainEventSource>(() => new DomainEventSource());

        public static DomainEventSource Log { get { return Instance.Value; } }

        private DomainEventSource() { }

        public class Keywords
        {
            public const EventKeywords Page = (EventKeywords)1;
            public const EventKeywords Database = (EventKeywords)2;
            public const EventKeywords Diagnostic = (EventKeywords)4;
            public const EventKeywords Perf = (EventKeywords)8;
        }

        public class Tasks
        {
            public const EventTask Page = (EventTask)1;
            public const EventTask DbQuery = (EventTask)2;
        }

        [Event(1, Message = "Application Failure: {0}",
            Level = EventLevel.Critical, Keywords = Keywords.Database, Task = Tasks.DbQuery)]
        public void Failure(string message)
        {
            WriteEvent(1, message);
        }

        [Event(2, Message = "Database Update Error: {0}",
            Level = EventLevel.Error, Keywords = Keywords.Database, Task = Tasks.DbQuery)]
        public void DbUpdateError(string message)
        {
            WriteEvent(2, message);
        }

        [Event(3, Message = "Database validation Error: {0}",
            Level = EventLevel.Error, Keywords = Keywords.Database, Task = Tasks.DbQuery)]
        public void DbValidationError(string message)
        {
            WriteEvent(3, message);
        }

        [Event(4, Message = "Repository Error: {0}",
            Level = EventLevel.Error, Keywords = Keywords.Diagnostic, Task = Tasks.DbQuery)]
        public void Error(string message)
        {
            WriteEvent(4, message);
        }

        [Event(5, Message = "Info: {0}",
            Level = EventLevel.Informational, Keywords = Keywords.Perf, Task = Tasks.DbQuery)]
        public void Info(string message)
        {
            WriteEvent(5, message);
        }
    }
}