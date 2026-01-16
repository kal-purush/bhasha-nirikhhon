using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using Application.IServices.Identity;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Tokens;

namespace Infrastructure.Services.Identity;

public class TokenService : ITokenService
{
    private readonly IConfiguration _configuration;

    private readonly ILogger _logger;

    public TokenService(
        IConfiguration configuration, 
        ILogger<TokenService> logger)
    {
        this._configuration = configuration;
        this._logger = logger;
    }

    public string GenerateAccessToken(IEnumerable<Claim> claims)
    {
        var tokenOptions = GetTokenOptions(claims);
        var tokenString = new JwtSecurityTokenHandler().WriteToken(tokenOptions);

        this._logger.LogInformation($"Generated new access token.");

        return tokenString;
    }

    public string GenerateRefreshToken()
    {
        var randomNumber = new byte[32];
        using var rng = RandomNumberGenerator.Create();
        rng.GetBytes(randomNumber);
        var refreshToken = Convert.ToBase64String(randomNumber);

        this._logger.LogInformation($"Generated new refresh token.");

        return refreshToken;
    }

    private JwtSecurityToken GetTokenOptions(IEnumerable<Claim> claims)
    {
        var secretKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(
            _configuration.GetValue<string>("JsonWebTokenKeys:IssuerSigningKey")));
        var signinCredentials = new SigningCredentials(secretKey, SecurityAlgorithms.HmacSha256);

        var tokenOptions = new JwtSecurityToken(
            issuer: _configuration.GetValue<string>("JsonWebTokenKeys:ValidIssuer"),
            audience: _configuration.GetValue<string>("JsonWebTokenKeys:ValidAudience"),
            expires: DateTime.UtcNow.AddMinutes(15),
            claims: claims,
            signingCredentials: signinCredentials
        );

        return tokenOptions;
    }
}