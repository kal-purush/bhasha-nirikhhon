namespace API.Filters;

public class AuthMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<AuthMiddleware> _logger;

    public AuthMiddleware(RequestDelegate next, ILogger<AuthMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }

    public async Task Invoke(HttpContext httpContext)
    {
        var accessToken = httpContext.Request.Cookies["AccessToken"];
        var refreshToken = httpContext.Request.Cookies["RefreshToken"];

        if (string.IsNullOrEmpty(accessToken) && string.IsNullOrEmpty(refreshToken))
        {
            _logger.LogWarning("No Access or Refresh token found in cookies.");
            httpContext.Response.StatusCode = StatusCodes.Status401Unauthorized;
            await httpContext.Response.WriteAsync("Unauthorized: Missing tokens.");
            return;
        }

        _logger.LogInformation("Access and Refresh tokens found in cookies.");
        
        // You could add additional logic to validate the token here if needed.

        // Proceed to the next middleware in the pipeline
        await _next(httpContext);
    }
}