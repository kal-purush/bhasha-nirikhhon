using Microsoft.AspNetCore.Http;
using System;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Security.Claims;

namespace KlockanAPI.Infrastructure.CrossCutting.Authorization;

public class JwtTokenHelper
{
    public static bool HasRequiredRole(HttpContext httpContext, string requiredRole)
    {
        string jwtToken = GetJwtTokenFromHeaders(httpContext.Request.Headers);
        if (jwtToken == null)
        {
            // No token found in headers
            return false;
        }

        var tokenHandler = new JwtSecurityTokenHandler();
        try
        {
            var token = tokenHandler.ReadJwtToken(jwtToken);

            // Check if the token contains the required role claim
            var rolesClaim = token.Claims.FirstOrDefault(c => c.Type == "roles");
            if (rolesClaim == null)
            {
                // No roles claim found
                return false;
            }

            // Deserialize the roles claim value
            var roles = rolesClaim.Value.Split(',');

            // Check if the required role is present in the roles claim
            return roles.Contains(requiredRole);
        }
        catch (Exception)
        {
            // Token validation failed
            return false;
        }
    }

    private static string GetJwtTokenFromHeaders(IHeaderDictionary headers)
    {
        string authorizationHeader = headers["Authorization"];
        if (string.IsNullOrWhiteSpace(authorizationHeader))
        {
            // No Authorization header found
            return null;
        }

        // Check if the Authorization header contains a JWT bearer token
        if (authorizationHeader.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
        {
            // Extract the JWT token
            return authorizationHeader.Substring("Bearer ".Length).Trim();
        }

        return null;
    }
}