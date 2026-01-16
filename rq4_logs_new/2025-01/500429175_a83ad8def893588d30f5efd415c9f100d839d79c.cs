using System.Security.Claims;
using Keycloak.Common.Extensions;
using Keycloak.Common.Options;
using Keycloak.Common.Services;
using Keycloak.Common.Services.Models;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;

namespace Keycloak.Common.Middlewares;

public class RoleMapperMiddleware(RequestDelegate next)
{
    public async Task InvokeAsync(HttpContext context)
    {
        await GetCallerRoles(context);
        await next(context);
    }

    public async Task GetCallerRoles(HttpContext context)
    {
        var requestContext =
            context.RequestServices.GetService(typeof(IHttpRequestContextService)) as IHttpRequestContextService;
        
        if (requestContext == null) 
            return;

        if (!requestContext.HasJwtToken)
            return;
        
        if (context.User.Identity is not ClaimsIdentity claimsIdentity)
            return;
        
        var result = await CallUserInfoEndpoint(context, requestContext);
        if (result == null)
            return;
        
        HandleRealmRoles(result, claimsIdentity);
    }

    private void HandleRealmRoles(UserInfoEndpointResponse result, ClaimsIdentity claimsIdentity)
    {
        foreach (var role in result.RealmRoles)
        {
            claimsIdentity.AddClaim(new Claim(ClaimTypes.Role, role));
        }
    }

    private HttpClient GetKeycloakHttpClient(HttpContext context, IHttpRequestContextService requestContext)
    {
        var httpFactory = context.RequestServices.GetService(typeof(IHttpClientFactory)) as IHttpClientFactory;
        var client = httpFactory.CreateClient();
        client.DefaultRequestHeaders.Add("Authorization", requestContext.Jwt);
        return client;
    }

    private async Task<UserInfoEndpointResponse?> CallUserInfoEndpoint(
        HttpContext context, IHttpRequestContextService requestContext)
    {
        var options =
            context.RequestServices.GetService(typeof(IOptions<KeycloakTokenOptions>)) as
                IOptions<KeycloakTokenOptions>;

        if (options == null)
            return null;
        
        var client = GetKeycloakHttpClient(context, requestContext);
        var userinfoPath = Path.Join(options.Value.AuthorizationServerUrl, "/protocol/openid-connect/userinfo");
        var response = await client.GetAsync(userinfoPath);
        
        if(!response.IsSuccessStatusCode) 
            return null;
        
        return await response.HandleResponse<UserInfoEndpointResponse>();
    }
}

public static class RoleMiddlewareExtensions
{
    public static IApplicationBuilder UseCallerRoleMapperMiddleware(this IApplicationBuilder builder)
    {
        return builder.UseMiddleware<RoleMapperMiddleware>();
    }
}