using MediatR;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace ForkAndSpoon.Application.Behaviors
{
    public class LoggingBehavior<TRequest, TResponse> : IPipelineBehavior<TRequest, TResponse>
           where TRequest : notnull // Added 'notnull' constraint to match the interface requirement  
    {
        private readonly ILogger<LoggingBehavior<TRequest, TResponse>> _logger;

        public LoggingBehavior(ILogger<LoggingBehavior<TRequest, TResponse>> logger)
        {
            _logger = logger;
        }

        public async Task<TResponse> Handle(TRequest request, RequestHandlerDelegate<TResponse> next, CancellationToken cancellationToken)
        {
            var requestName = request.GetType();
            _logger.LogInformation("{Request} is starting.", requestName);
            var timer = Stopwatch.StartNew();

            var response = await next();
            timer.Stop();
            _logger.LogInformation("{Request} has finished in {Time}ms.", requestName, timer.ElapsedMilliseconds);
            return response;
        }
    }
}