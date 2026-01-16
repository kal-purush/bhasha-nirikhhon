using DS.Dtos.ResponseMessages;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.Extensions.Logging;
using System.Net;

namespace DS.Infrastructure.Web.Filters
{
    public class GlobalExceptionFilter : IExceptionFilter
    {
        private readonly ILogger _logger;

        public GlobalExceptionFilter(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger(nameof(GlobalExceptionFilter));
        }

        public void OnException(ExceptionContext context)
        {
            _logger.LogError(context.Exception, context.Exception.Message);

            context.Result = BuildObjectResult(ResponseMessages.Handler.InternalServerError);
        }

        private ObjectResult BuildObjectResult(params string[] messages)
        {
            return new ObjectResult(OperationResponse.InternalServerError(messages)) { StatusCode = (int)HttpStatusCode.InternalServerError };
        }
    }
}