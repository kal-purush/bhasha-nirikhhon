using Microsoft.AspNetCore.Mvc;
using WebAPI.Helpers;

namespace WebAPI.Middlewares;

public class DeveloperExceptionHandlerMiddleware : IMiddleware
{
    public async Task InvokeAsync(HttpContext context, RequestDelegate next)
    {
        try
        {
            await next(context);
        }
        catch (Exception e)
        {
            await ExceptionMiddlewareHelper.SetErrorResponse(
                new ProblemDetails
                {
                    Status = StatusCodes.Status500InternalServerError,
                    Type = "https://datatracker.ietf.org/doc/html/rfc7231#section-6.6.1",
                    Title = e.Message,
                    Detail = e.InnerException!.Message + Environment.NewLine + e.StackTrace,
                },
                context
            );
        }
    }
}