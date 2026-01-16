using Mambo.Response;
using Microsoft.AspNetCore.Diagnostics;
using System.Net;
using System.Net.Mime;

namespace CoreService.API
{
    public class GlobalExceptionHandler(ILogger<GlobalExceptionHandler> logger) : IExceptionHandler
    {
        private readonly ILogger<GlobalExceptionHandler> _logger = logger;

        public async ValueTask<bool> TryHandleAsync(HttpContext httpContext, Exception exception, CancellationToken cancellationToken)
        {
            _logger.LogCritical("An unexpected error occurred in the server. Error: {@Error}. Status: {@Status}", exception, HttpStatusCode.InternalServerError);
            httpContext.Response.Clear();
            httpContext.Response.StatusCode = StatusCodes.Status500InternalServerError;
            httpContext.Response.ContentType = MediaTypeNames.Application.Json;
            var response = BaseErrorResponse.CreateNewBaseErrorResponse(exception.Message, httpContext.TraceIdentifier);
            await httpContext.Response.WriteAsJsonAsync(response, cancellationToken);
            return true;
        }
    }
}