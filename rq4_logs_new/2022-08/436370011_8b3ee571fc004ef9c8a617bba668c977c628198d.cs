using Microsoft.AspNetCore.Mvc.Filters;

namespace Zastai.NuGet.Server.Services;

/// <summary>A service filter that takes care of logging all requests and whether or not they succeeded.</summary>
public sealed class RequestLoggingFilter : IAsyncActionFilter, IAsyncExceptionFilter, IAsyncResultFilter {

  private readonly ILogger<RequestLoggingFilter> _logger;

  // FIXME: Perhaps this could/should also try to take care of counting requests, for server statistics purposes?

  /// <summary>Creates a request logging filter.</summary>
  /// <param name="logger">The logger service to use.</param>
  public RequestLoggingFilter(ILogger<RequestLoggingFilter> logger) {
    this._logger = logger;
  }

  /// <inheritdoc />
  public async Task OnActionExecutionAsync(ActionExecutingContext context, ActionExecutionDelegate next) {
    var r = context.HttpContext.Request;
    if (r.ContentType is null && r.ContentLength is null) {
      this._logger.LogTrace("Request <{id}>: {protocol} {method} {path} (no content).", context.HttpContext.TraceIdentifier,
                            r.Protocol, r.Method, r.Path);
    }
    else {
      this._logger.LogTrace("Request <{id}>: {protocol} {method} {path} ({contentType}; {contentLength} bytes).",
                            context.HttpContext.TraceIdentifier, r.Protocol, r.Method, r.Path, r.ContentType, r.ContentLength);
    }
    await next();
  }

  /// <inheritdoc />
  public async Task OnExceptionAsync(ExceptionContext context) {
    this._logger.LogTrace("Request <{id}> completed with an exception: {ex}", context.HttpContext.TraceIdentifier,
                          context.Exception);
    await Task.Yield();
  }

  /// <inheritdoc />
  public async Task OnResultExecutionAsync(ResultExecutingContext context, ResultExecutionDelegate next) {
    await next();
    var ctx = context.HttpContext;
    var r = ctx.Response;
    if (r.ContentLength is null) {
      this._logger.LogTrace("Request <{id}> completed with status {status} ({contentType}).", ctx.TraceIdentifier, r.StatusCode,
                            r.ContentType);
    }
    else {
      this._logger.LogTrace("Request <{id}> completed with status {status} ({contentType}; {contentLength} bytes).",
                            ctx.TraceIdentifier, r.StatusCode, r.ContentType, r.ContentLength);
    }
  }

}