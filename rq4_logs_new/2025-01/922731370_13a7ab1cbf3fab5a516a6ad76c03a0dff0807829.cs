using Sample.Domain.Constants;
using Sample.Domain.CustomExceptions;
using System.Net;
using System.Text.Json;

namespace Sample.Api.Middlewares;

public class ExceptionMiddleware(RequestDelegate next, IHostEnvironment env)
{
    private readonly RequestDelegate _next = next;
    private readonly IHostEnvironment _env = env;
    public async Task InvokeAsync(HttpContext context)
    {
        try
        {
            await _next(context);
        }
        catch (Exception ex)
        {
            await HandleExceptionAsync(context, ex, _env);
        }
    }

    private static async Task HandleExceptionAsync(HttpContext context, Exception ex, IHostEnvironment env)
    {
        context.Response.ContentType = MiddlewareConstants.JSON;
        int statusCode = (int)HttpStatusCode.InternalServerError;
        string result = string.Empty;

        switch (ex)
        {
            case InvalidOperationException invalidOperationException:
                result = JsonSerializer.Serialize(new GenericException(invalidOperationException.Message, MiddlewareConstants.INVALID_CODE_ERROR));
                context.Response.StatusCode = statusCode;
                break;
            case UnauthorizedAccessException unauthorizedAccessException:
                result = JsonSerializer.Serialize(new GenericException(unauthorizedAccessException.Message, MiddlewareConstants.UNAUTHORIZED_CODE_ERROR));
                context.Response.StatusCode = statusCode;
                break;
            case FormatException formatException:
                result = JsonSerializer.Serialize(new GenericException(formatException.Message, MiddlewareConstants.FORMART_CODE_ERROR));
                context.Response.StatusCode = statusCode;
                break;
            case TimeoutException timeOutException:
                result = JsonSerializer.Serialize(new GenericException(timeOutException.Message, MiddlewareConstants.TIME_OUT_CODE_ERROR));
                context.Response.StatusCode = statusCode;
                break;
            case IOException IOException:
                result = JsonSerializer.Serialize(new GenericException(IOException.Message, MiddlewareConstants.IO_CODE_ERROR));
                context.Response.StatusCode = statusCode;
                break;
        }
        if (string.IsNullOrEmpty(result) &&  env.IsDevelopment())
        {
            result = JsonSerializer.Serialize(new GenericException(ex.Message, ex.StackTrace ?? string.Empty));
            context.Response.StatusCode = statusCode;
        }
        else if(string.IsNullOrEmpty(result) && env.IsProduction())
        {
            result = JsonSerializer.Serialize(new GenericException(ex.Message, string.Empty));
            context.Response.StatusCode = statusCode;
        }

        await context.Response.WriteAsync(result);
    }
}