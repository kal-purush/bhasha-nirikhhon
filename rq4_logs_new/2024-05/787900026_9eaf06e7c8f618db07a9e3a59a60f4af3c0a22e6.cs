using System.IdentityModel.Tokens.Jwt;

namespace MovieAPI.WebAPI.Helper;

public static class JwtHelper
{
    public static string GetUserIdFromToken(HttpContext context)
    {
        var authorizationHeader = context.Request.Headers["Authorization"].FirstOrDefault();
        if (!string.IsNullOrEmpty(authorizationHeader) && authorizationHeader.StartsWith("Bearer "))
        {
            var token = authorizationHeader.Substring("Bearer ".Length).Trim();
            var handler = new JwtSecurityTokenHandler();
            var jsonToken = handler.ReadToken(token) as JwtSecurityToken;
            return jsonToken?.Claims.FirstOrDefault(c => c.Type == "Id")?.Value;
        }

        return null;
    }
}
