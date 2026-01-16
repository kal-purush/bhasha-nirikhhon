using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;

namespace Application.Commons.Services.TokenService;

public class TokenService : ITokenService
{
    private readonly AuthSetting _authSetting;

    public TokenService(IOptions<AuthSetting> authSetting)
    {
        _authSetting = authSetting.Value;
    }

    public string GenerateHomederToken(HomederTokenData homederTokenData)
    {
        string userEmail =  homederTokenData.UserEmail ?? "";
        var key = Encoding.ASCII.GetBytes(_authSetting.HomederTokenSecret);
        var tokenHandler = new JwtSecurityTokenHandler();
        var tokenDescriptor = new SecurityTokenDescriptor
        {
            Subject = new ClaimsIdentity(new [] 
            {
                new Claim("id", homederTokenData.UserId.ToString()),
                new Claim("role", homederTokenData.UserRole.ToString()),
                new Claim(JwtRegisteredClaimNames.Sub, userEmail),
                new Claim(JwtRegisteredClaimNames.Jti, Guid.NewGuid().ToString()),
                new Claim(JwtRegisteredClaimNames.Email, userEmail), 
            }),
            Expires = DateTime.UtcNow.AddHours(2),
            SigningCredentials = new SigningCredentials(new SymmetricSecurityKey(key), SecurityAlgorithms.HmacSha256Signature)
        };
        var secutiryToken = tokenHandler.CreateToken(tokenDescriptor);
        var jwtToken = tokenHandler.WriteToken(secutiryToken);
        return jwtToken;
    }

    public GoogleTokenData DecodeGoogleToken(string googleToken)
    {
        JwtSecurityTokenHandler handler = new JwtSecurityTokenHandler();
        JwtSecurityToken token = handler.ReadJwtToken(googleToken);

        var data = new GoogleTokenData
        {
            Aud = token.Claims.FirstOrDefault(claim => claim.Type == "aud")?.Value,
            Name = token.Claims.FirstOrDefault(claim => claim.Type == "given_name")?.Value,
            Surname = token.Claims.FirstOrDefault(claim => claim.Type == "family_name")?.Value,
            PhotoUrl = token.Claims.FirstOrDefault(claim => claim.Type == "picture")?.Value,
            Email = token.Claims.FirstOrDefault(claim => claim.Type == "email")?.Value,
            IsEmailVerified = bool.Parse(token.Claims.FirstOrDefault(claim => claim.Type == "email_verified")?.Value),
        };

        return data;
    }

    public bool ValidateGoogleTokenAsync(string googleToken)
    {
        try
        {
            var googleTokenData = DecodeGoogleToken(googleToken);
            return googleTokenData.Aud.Equals(_authSetting.GoogleTokenClientId);
        }
        catch (Exception)
        {
            return false;
        }
    }
}