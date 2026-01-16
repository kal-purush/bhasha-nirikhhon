using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace Bashi.Tests.Framework.Logging
{
    public class TestableLogger<T> : TestableLogger, ILogger<T>
    {
    }

    public class TestableLogger : ILogger
    {
        private readonly List<TestLogEntry> logs = new List<TestLogEntry>();
        public IReadOnlyList<TestLogEntry> LogEntries => this.logs.AsReadOnly();

        public void Log<TState>(LogLevel logLevel,
                                EventId eventId,
                                TState state,
                                Exception exception,
                                Func<TState, Exception, string> formatter)
        {
            this.logs.Add(new TestLogEntry(logLevel, exception, formatter(state, exception)));
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public IDisposable BeginScope<TState>(TState state)
        {
            return new EmptyDisposable();
        }

        private class EmptyDisposable : IDisposable
        {
            public void Dispose()
            {
            }
        }
    }
}