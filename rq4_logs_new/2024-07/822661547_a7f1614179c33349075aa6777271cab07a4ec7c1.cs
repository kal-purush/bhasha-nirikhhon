using Microsoft.AspNetCore.Mvc;
using ShortSharing.API.Constants;
using ShortSharing.BLL.Common.Exceptions;
using System.Net;
using System.Text.Json;

namespace ShortSharing.API.Middlewares;

public class ExceptionMiddleware : IMiddleware
{
    private readonly RequestDelegate _next;

    public ExceptionMiddleware(RequestDelegate next)
    {
        _next = next; 
    }

    public async Task InvokeAsync(HttpContext httpContext, RequestDelegate next)
    {
        try
        {
            await _next.Invoke(httpContext);
        }
        catch (Exception e)
        {
            await HandleException(httpContext, e);
        }
    }

    private async Task HandleException(HttpContext httpContext, Exception exception)
    {
        HttpStatusCode statusCode = HttpStatusCode.InternalServerError;

        string message = string.Empty;

        switch (exception)
        {
            case NotFoundException notFoundException:
                statusCode = HttpStatusCode.NotFound;
                message = notFoundException.Message; 
                break;
            case BadRequestException badRequestException:
                statusCode = HttpStatusCode.BadRequest;
                message = badRequestException.Message; 
                break;
            case ForbiddenException forbiddenException:
                statusCode = HttpStatusCode.Forbidden;
                message = forbiddenException.Message; 
                break;
            default:
                statusCode = HttpStatusCode.InternalServerError;
                message = exception.Message; 
                break;
        }

        ProblemDetails problem = new()
        {
            Status = (int)statusCode,
            Title = message,
        };

        string jsonProblem = JsonSerializer.Serialize(problem);

        httpContext.Response.ContentType = ApiConstants.ContentType;

        await httpContext.Response.WriteAsync(jsonProblem);
    }
}