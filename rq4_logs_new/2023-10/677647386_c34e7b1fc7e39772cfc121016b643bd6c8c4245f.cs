using System.Net;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Authorization.Policy;
using TsdDelivery.Api.Contracts.Common;
using TsdDelivery.Application.Models;

namespace TsdDelivery.Api.Middlewares;

public class AuthorizationMiddlewareHandler : IAuthorizationMiddlewareResultHandler
{
    private readonly AuthorizationMiddlewareResultHandler DefaultHandler = new AuthorizationMiddlewareResultHandler();
    public async Task HandleAsync(RequestDelegate next, HttpContext context, AuthorizationPolicy policy,
        PolicyAuthorizationResult authorizeResult)
    {
        if(authorizeResult.Challenged)
        {
            context.Response.StatusCode = (int)HttpStatusCode.Unauthorized;
            //response body
            await context.Response.WriteAsJsonAsync(new ErrorResponse(){
                StatusCode = (int)ErrorCode.UnAuthorize,
                StatusPhrase = "UnAuthorize",
                Errors = new List<string>() {"UnAuthorized: Access is Denied due invalid credential"},
                Timestamp = DateTime.UtcNow.AddHours(7)
            });
            return;
        }
        if (authorizeResult.Forbidden)
        {
            context.Response.StatusCode = (int)HttpStatusCode.Forbidden;
            //response body
            await context.Response.WriteAsJsonAsync(new ErrorResponse(){
                StatusCode = (int)ErrorCode.Forbidden,
                StatusPhrase = "Forbidden",
                Errors = new List<string>() {"Permission: You do not have permission to access this resource"},
                Timestamp = DateTime.UtcNow.AddHours(7)
            });
            return;
        }
        await DefaultHandler.HandleAsync(next, context, policy, authorizeResult);
    }
}