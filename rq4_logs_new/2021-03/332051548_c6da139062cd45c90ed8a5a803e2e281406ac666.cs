using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Order.Shared.Dto;

namespace Order.Server.Security
{
    public class ExternalProviderSigninRedirectionMiddleware
    {
        private readonly RequestDelegate requestDelegate;

        public ExternalProviderSigninRedirectionMiddleware(RequestDelegate requestDelegate)
        {
            this.requestDelegate = requestDelegate;
        }

        public async Task Invoke(HttpContext context, ILogger<ExternalProviderSigninRedirectionMiddleware> logger)
        {
            await requestDelegate.Invoke(context);

            if (context.Response.StatusCode == (int)HttpStatusCode.Redirect)
            {
                var redirectUrl = context.Response.Headers["Location"].FirstOrDefault();
                if (!string.IsNullOrWhiteSpace(redirectUrl))
                {
                    if (
                        redirectUrl.StartsWith("https://accounts.google.com/o/oauth2/v2/auth")
                     || redirectUrl.StartsWith("https://www.linkedin.com/oauth/v2/authorization")
                     || redirectUrl.StartsWith("https://www.facebook.com/v8.0/dialog/oauth"))
                    {
                        context.Response.StatusCode = (int)HttpStatusCode.OK;
                        await context.Response.WriteAsJsonAsync<ValueWrapperDto<string>>(
                            new ValueWrapperDto<string>(redirectUrl)
                        );
                        logger.LogInformation($"An Attempt to signin with an external provider was detected. Rewriting the response to pass the redirection URL {redirectUrl}");
                    }
                }
            }
        }
    }
}