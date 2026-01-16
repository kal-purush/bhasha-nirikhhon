using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;

namespace CarsInfo.WebApi.Authorization
{
    public class JwtTokenFactory : ITokenFactory
    {
        private readonly ApiAuthSetting _settings;
        private readonly SigningCredentials _credentials;

        public JwtTokenFactory(IOptions<ApiAuthSetting> settings)
        {
            _settings = settings.Value;
            var securityKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(_settings.Secret));
            _credentials = new SigningCredentials(securityKey, SecurityAlgorithms.HmacSha256);
        }

        public string CreateToken(ICollection<Claim> claims)
        {
            var now = DateTime.UtcNow;
            var jwt = new JwtSecurityToken(
                _settings.Issuer,
                notBefore: now,
                claims: claims,
                expires: now.Add(TimeSpan.FromMinutes(_settings.ExpirationTime)),
                signingCredentials: _credentials);
            var tokenHandler = new JwtSecurityTokenHandler();

            return tokenHandler.WriteToken(jwt);
        }
    }
}