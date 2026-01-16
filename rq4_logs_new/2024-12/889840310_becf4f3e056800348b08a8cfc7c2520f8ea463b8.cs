namespace TaskManagementApp.Api.Middlewares;

public sealed class HealtCheckMiddleware
{
    #region Fields
    private readonly RequestDelegate _next;
    #endregion

    #region Constructors
    public HealtCheckMiddleware(RequestDelegate next)
    {
        _next= next;
    }
    #endregion

    #region Methods
    public async Task InvokeAsync(HttpContext httpContext)
    {
        if (httpContext.Request.Path == "/healthCheck")
        {
            httpContext.Response.Clear();
            httpContext.Response.StatusCode = StatusCodes.Status200OK;
            await httpContext.Response.WriteAsync("Server is healthy");
            await httpContext.Response.CompleteAsync();
        }
        else
        {
            await _next(httpContext);
        }
    }
    #endregion
}