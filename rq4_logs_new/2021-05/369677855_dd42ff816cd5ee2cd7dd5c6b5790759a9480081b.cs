using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;
using AtmosAQ.Infrastructure.Identity.Models;
using Microsoft.AspNetCore.Identity;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using ITokenService = AtmosAQ.Infrastructure.Identity.Services.ITokenService;
using JwtRegisteredClaimNames = Microsoft.IdentityModel.JsonWebTokens.JwtRegisteredClaimNames;

namespace AtmosAQ.Infrastructure.Identity.Services
{
    public class TokenService : ITokenService
    {
        private readonly UserManager<ApplicationUser> _userManager;
        private readonly JwtToken _tokenOptions;

        public TokenService(
            UserManager<ApplicationUser> userManager,
            IOptions<JwtToken> tokenOptions)
        {
            _userManager = userManager;
            _tokenOptions = tokenOptions.Value;
        }

        public async Task<AuthenticationResponse> Authenticate(AuthenticationRequest request)
        {
            var authResult = await IsValidUser(request.Username, request.Password);

            if (authResult)
            {
                var user = await _userManager.FindByNameAsync(request.Username);

                var jwtToken = CreateJwtToken(user);

                return new AuthenticationResponse(user,
                    jwtToken);
            }
            
            return null;
        }

        private async Task<bool> IsValidUser(string username, string password)
        {
            var user = await _userManager.FindByNameAsync(username);

            if (user is null)
            {
                return false;
            }

            var authResult = await _userManager.CheckPasswordAsync(user, password);

            return authResult;
        }
        
        private string CreateJwtToken(ApplicationUser user)
        {
            var tokenClaims = new List<Claim>
            {
                new Claim(ClaimTypes.NameIdentifier, user.Id),
                new Claim(JwtRegisteredClaimNames.Jti, Guid.NewGuid().ToString())
            };
            
            var tokenIssuer = _tokenOptions.Issuer;
            var tokenAudience = _tokenOptions.Audience;
            var tokenKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(_tokenOptions.Secret));
            var tokenCreds = new SigningCredentials(tokenKey, SecurityAlgorithms.HmacSha256);
            var tokenExpires = DateTime.Now.AddMinutes(_tokenOptions.Expiry);
            
            JwtSecurityToken token = new JwtSecurityToken(
                issuer: tokenIssuer,
                audience: tokenAudience,
                signingCredentials: tokenCreds,
                claims: tokenClaims,
                expires: tokenExpires
            );
            
            return new JwtSecurityTokenHandler().WriteToken(token);
        }
    }
}