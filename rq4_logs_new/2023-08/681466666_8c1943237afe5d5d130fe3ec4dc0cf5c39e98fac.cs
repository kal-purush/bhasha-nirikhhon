using BetterCoding.MessagePubSubCenter.Services;
using System.Net;

namespace BetterCoding.MessagePubSubCenter.API.Middlewares
{
    public class APIKeyMiddleware
    {
        private readonly RequestDelegate _next;
        private readonly string APIKeyHeaderName = "x-bc-apiKey";
        private IAPIKeyAuthenticate _apiKeyAuthenticate;
        public APIKeyMiddleware(RequestDelegate next,
            IAPIKeyAuthenticate apiKeyAuthenticate)
        {
            _next = next;
            _apiKeyAuthenticate = apiKeyAuthenticate;
        }

        public async Task InvokeAsync(HttpContext context)
        {
            if (string.IsNullOrWhiteSpace(context.Request.Headers[APIKeyHeaderName]))
            {
                context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                return;
            }
            string? userApiKey = context.Request.Headers[APIKeyHeaderName];
            if (!_apiKeyAuthenticate.IsValidApiKey(userApiKey!))
            {
                context.Response.StatusCode = (int)HttpStatusCode.Unauthorized;
                return;
            }
            await _next(context);
        }
    }

    public static class APIKeyMiddlewareExtensions
    {
        public static WebApplication UseAPIKeyMiddleware(this WebApplication app, IConfiguration configuration)
        {
            var configed = configuration.GetValue<string>("APIKey");
            if (string.IsNullOrEmpty(configed))
                return app;
            app.UseAuthentication();
            app.UseMiddleware<APIKeyMiddleware>();
            return app;
        }
    }
}