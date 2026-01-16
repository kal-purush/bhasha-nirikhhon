using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using CarsInfo.Application.BusinessLogic.AuthModels;
using CarsInfo.Application.BusinessLogic.Contracts;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;

namespace CarsInfo.Infrastructure.BusinessLogic.Services
{
    public class TokenService : ITokenService
    {
        private readonly ApiAuthSetting _authSetting;

        public TokenService(IOptions<ApiAuthSetting> authSetting)
        {
            _authSetting = authSetting.Value;
        }

        public string GenerateAccessToken(IEnumerable<Claim> claims)
        {
            var securityKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(_authSetting.Secret));
            var credentials = new SigningCredentials(securityKey, SecurityAlgorithms.HmacSha256);
            var now = DateTime.Now;
            var jwt = new JwtSecurityToken(
                _authSetting.Issuer,
                notBefore: now,
                claims: claims,
                expires: now.Add(TimeSpan.FromSeconds(_authSetting.ExpirationTime)),
                signingCredentials: credentials);
            var tokenHandler = new JwtSecurityTokenHandler();

            return tokenHandler.WriteToken(jwt);
        }

        public string GenerateRefreshToken()
        {
            var randomNumber = new byte[32];
            using (var rng = RandomNumberGenerator.Create())
            {
                rng.GetBytes(randomNumber);
                return Convert.ToBase64String(randomNumber);
            }
        }

        public ClaimsPrincipal GetPrincipalFromExpiredToken(string token)
        {
            var tokenValidationParameters = new TokenValidationParameters
            {
                RequireExpirationTime = true,
                ValidateLifetime = true,
                ValidateIssuerSigningKey = true,
                IssuerSigningKey = new SymmetricSecurityKey(Encoding.ASCII.GetBytes(_authSetting.Secret)),
                ValidIssuer = _authSetting.Issuer,
                ValidateIssuer = true,
                ValidateAudience = false
            };
            var tokenHandler = new JwtSecurityTokenHandler();
            var principal = tokenHandler.ValidateToken(token, tokenValidationParameters, out var securityToken);

            if (securityToken is not JwtSecurityToken jwtSecurityToken ||
                !jwtSecurityToken.Header.Alg.Equals(SecurityAlgorithms.HmacSha256, 
                    StringComparison.InvariantCultureIgnoreCase))
            {

                throw new SecurityTokenException("Invalid token");
            }

            return principal;
        }
    }
}