using Crypto.Core.Exceptions;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Mvc;

namespace Crypto.API.Handlers;

public class GlobalExceptionHandler(IProblemDetailsService detailsService) : IExceptionHandler
{
    public async ValueTask<bool> TryHandleAsync(HttpContext httpContext, Exception exception, CancellationToken cancellationToken)
    {
        httpContext.Response.StatusCode = exception switch
        {
            CryptoCoreNotFoundException => StatusCodes.Status404NotFound,
            CryptoCoreException => StatusCodes.Status400BadRequest,
            _ => StatusCodes.Status500InternalServerError
        };

        var problemDetails = CreateProblemDetails(httpContext, exception);

        return await detailsService.TryWriteAsync(new ProblemDetailsContext
        {
            HttpContext = httpContext,
            Exception = exception,
            ProblemDetails = problemDetails
        });
    }

    private ProblemDetails CreateProblemDetails(HttpContext httpContext, Exception exception)
    {
        var title = exception switch
        {
            CryptoCoreValidationException => "Validation exception.",
            CryptoCoreNotFoundException => "Item not found.",
            _ => "An problem occurred."
        };

        ProblemDetails? problemDetails = null;
        
        if (exception is CryptoCoreValidationException validationException)
        {
            problemDetails = new ValidationProblemDetails()
            {
                Errors = validationException.Errors
            };
        }

        problemDetails ??= new ProblemDetails();
        
        problemDetails.Title = title;
        problemDetails.Type = exception.GetType().Name;
        problemDetails.Detail = exception.Message;
        problemDetails.Instance = $"{httpContext.Request.Method} {httpContext.Request.Path}";
        problemDetails.Extensions = new Dictionary<string, object?>
        {
            { "requestId", httpContext.TraceIdentifier }
        };

        return problemDetails;
    }
}