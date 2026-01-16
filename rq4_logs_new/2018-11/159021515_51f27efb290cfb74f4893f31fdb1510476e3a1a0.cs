using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Internal;
using NSubstitute;
using System;

namespace DS.Handlers.UnitTests.Extensions
{
    public static class ILoggerExtensions
    {
        public static void ShouldBeCalled(this ILogger logger, LogLevel level, int numberOfCalls)
        {
            logger.Received(numberOfCalls).Log(
                level,
                Arg.Any<EventId>(),
                Arg.Any<FormattedLogValues>(),
                Arg.Any<Exception>(), Arg.Any<Func<object, Exception, string>>());
        }

        public static void ShouldBeCalledAndContainsMessage(this ILogger logger, LogLevel level, int numberOfCalls, string message)
        {
            logger.Received(numberOfCalls).Log(
                level,
                Arg.Any<EventId>(),
                Arg.Is<FormattedLogValues>(flv => flv.ToString().IndexOf(message, StringComparison.OrdinalIgnoreCase) > -1),
                Arg.Any<Exception>(), Arg.Any<Func<object, Exception, string>>());
        }
    }
}