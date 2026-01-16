namespace ShortSharing.API.Middlewares;

public class LoggingMiddleware 
{
    private readonly RequestDelegate _next;
    private readonly ILogger _logger;

    public LoggingMiddleware(RequestDelegate next, ILoggerFactory loggerFactory)
    {
        _next = next;
        _logger = loggerFactory.CreateLogger<LoggingMiddleware>();
    }

    public async void Invoke(HttpContext context) 
    {
        try
        {
            await _next(context);
        }
        finally 
        {
            LogRequest(context);
        }
    }

    private void LogRequest(HttpContext context)
    {
        var method = context.Request.Method;
        var path = context.Request.Path;
        var statusCode = context.Response.StatusCode;

        _logger.LogInformation(
            $"HTTP {method} {path} responded with {statusCode}"
        );
    }
}