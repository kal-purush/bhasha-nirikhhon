using System.IdentityModel.Tokens.Jwt;

namespace WebAPI.Auth;

public class JwtDecoder
{
    public GoogleTokenData DecodeGoogleToken(string googleToken)
    {
        JwtSecurityTokenHandler handler = new JwtSecurityTokenHandler();
        JwtSecurityToken token = handler.ReadJwtToken(googleToken);

        var data = new GoogleTokenData
        {
            Name = token.Claims.FirstOrDefault(claim => claim.Type == "given_name")?.Value,
            Surname = token.Claims.FirstOrDefault(claim => claim.Type == "family_name")?.Value,
            PhotoUrl = token.Claims.FirstOrDefault(claim => claim.Type == "picture")?.Value,
            Email = token.Claims.FirstOrDefault(claim => claim.Type == "email")?.Value,
            IsEmailVerified = bool.Parse(token.Claims.FirstOrDefault(claim => claim.Type == "email_verified")?.Value),
        };

        return data;
    }
}