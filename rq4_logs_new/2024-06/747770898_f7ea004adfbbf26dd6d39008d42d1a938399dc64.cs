using System.Security.Claims;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Extensions;
using NLogFlake.Constants;

namespace NLogFlake.Helpers;

internal static class HttpContextHelper
{
    internal static async Task<Dictionary<string, object>> GetLogParametersAsync(HttpContext httpContext, bool includeResponse)
    {
        string? request = await GetStringBody(httpContext.Request.Body);
        Dictionary<string, object> exceptionParams = new()
        {
            {"Request uri", new Uri(httpContext.Request.GetDisplayUrl())},
            {"Request method", httpContext.Request.Method},
            {"Request headers", httpContext.Request.Headers},
        };

        if (!string.IsNullOrWhiteSpace(request))
        {
            exceptionParams.Add("Request body", request);
        }

        if (includeResponse)
        {
            exceptionParams.Add("Response headers", httpContext.Response.Headers);
            exceptionParams.Add("Response status", httpContext.Response.StatusCode);

            string? response = await GetStringBody(httpContext.Response.Body);
            if (!string.IsNullOrWhiteSpace(response))
            {
                exceptionParams.Add("Response body", response);
            }
        }

        string? trace = httpContext.Items[HttpContextConstants.TraceContext]?.ToString();
        if (!string.IsNullOrWhiteSpace(trace))
        {
            exceptionParams.Add("Trace", trace);
        }

        return exceptionParams;
    }

    internal static Claim? GetClaim(HttpContext httpContext, string claim)
    {
        if (string.IsNullOrWhiteSpace(claim))
        {
            return null;
        }

        return httpContext.User?.Claims?.FirstOrDefault(_ => _.Type.Trim().Equals(claim.Trim(), StringComparison.CurrentCultureIgnoreCase));
    }

    internal static string? GetClaimValue(HttpContext httpContext, string claimName)
    {
        Claim? claim = GetClaim(httpContext, claimName);

        if (claim is null)
        {
            return null;
        }

        return claim.Value;
    }

    internal static string? GetClientId(HttpContext httpContext)
    {
        string? clientId = GetClaimValue(httpContext, HttpContextConstants.ClientIdOther);

        if (string.IsNullOrWhiteSpace(clientId))
        {
            clientId = GetClaimValue(httpContext, HttpContextConstants.ClientId);
        }

        return clientId;
    }

    internal static async Task<string> GetStringBody(Stream body)
    {
        using StreamReader bodyStream = new(body);
        bodyStream.BaseStream.Seek(0, SeekOrigin.Begin);

        string stringContent = await bodyStream.ReadToEndAsync();

        body.Position = 0;

        return stringContent;
    }
}