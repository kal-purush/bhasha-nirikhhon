using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using Colabora.Infrastructure.Auth.Exceptions;
using Colabora.Infrastructure.Auth.Google;
using Colabora.Infrastructure.Auth.Shared;
using Google.Apis.Auth;
using Microsoft.Extensions.Options;

namespace Colabora.Infrastructure.Auth;

public class AuthService : IAuthService
{
    private readonly IGoogleAuthService _googleAuthService;
    private readonly JwtSettings _jwtSettings;

    public AuthService(IGoogleAuthService googleAuthService, IOptions<JwtSettings> securityCredentialOptions)
    {
        _googleAuthService = googleAuthService;
        _jwtSettings = securityCredentialOptions.Value;
    }

    public async Task<AuthResult> Authenticate(AuthProvider authProvider, string token)
    {
        try
        {
            var userInfo = authProvider switch
            {
                AuthProvider.Google => await _googleAuthService.Authenticate(token),
                AuthProvider.Undefined or _ => throw new InvalidAuthProviderException("The given provider is invalid")
            };

            var jwt = new JwtSecurityToken(
                issuer: _jwtSettings.JwtIssuer,
                audience: _jwtSettings.JwtAudience,
                expires: DateTime.Now.Add(_jwtSettings.ExpirationTime),
                claims: new List<Claim>
                {
                    new(ClaimTypes.Email, userInfo.Email),
                    new(ClaimTypes.Name, userInfo.Name)
                });

            var stringJwt = new JwtSecurityTokenHandler().WriteToken(jwt);

            return new AuthResult {Email = userInfo.Email, Token = stringJwt};
        }
        catch (InvalidJwtException e)
        {
            return new AuthResult {Error = e.Message};
        }
    }
}