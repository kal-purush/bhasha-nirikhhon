using TaskManagementApp.Api.Repositories.Interfaces;

namespace TaskManagementApp.Api.Middlewares;

public class UnauthorizedTrackingMiddleware
{
    #region Fields
    private readonly RequestDelegate _next;
    private readonly IUnauthorizedTrackingMiddlewareRepository _unauthorizedTrackingMiddlewareRepository;
    #endregion

    #region Constructors

    public UnauthorizedTrackingMiddleware(RequestDelegate next, IUnauthorizedTrackingMiddlewareRepository unauthorizedTrackingMiddlewareRepository)
    {
        _next = next;
        _unauthorizedTrackingMiddlewareRepository = unauthorizedTrackingMiddlewareRepository;
        
    }
    #endregion

    #region Methods

    public async Task InvokeAsync(HttpContext context)
    {
        // Capture the original response body
        var originalBodyStream = context.Response.Body;

        try
        {
            using (var memoryStream = new MemoryStream())
            {
                context.Response.Body = memoryStream;

                // Proceed with the pipeline
                await _next(context);

                // Reset the memory stream position
                memoryStream.Seek(0, SeekOrigin.Begin);

                // Check if the response status code is 401
                if (context.Response.StatusCode == StatusCodes.Status401Unauthorized)
                {
                   await _unauthorizedTrackingMiddlewareRepository.CreateAsync(context, CancellationToken.None);
                }

                // Copy the response back to the original stream
                memoryStream.Seek(0, SeekOrigin.Begin);
                await memoryStream.CopyToAsync(originalBodyStream);
            }
        }
        finally
        {
            // Restore the original response body stream
            context.Response.Body = originalBodyStream;
        }
    }

    #endregion
}