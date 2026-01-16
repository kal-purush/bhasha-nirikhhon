using CvApp.Repositories.Exceptions;

namespace CvApp.Middleware;

public class ApiExceptionMiddleware
{
    private readonly RequestDelegate _next;

    public ApiExceptionMiddleware(RequestDelegate next)
    {
        _next = next;
    }

    public async Task InvokeAsync(HttpContext httpContext)
    {
        try
        {
            await _next(httpContext);
        }
        catch (EntityNotFoundException)
        {
            httpContext.Response.StatusCode = StatusCodes.Status404NotFound;
        }
    }
}