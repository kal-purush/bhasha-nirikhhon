using FoodZone.Models.Security;
using FoodZone.Services.DTO;
using FoodZone.Services.IServices;
using Microsoft.AspNetCore.Identity;
using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Tokens;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;

namespace FoodZone.Services.Services
{
    public class AuthServices : IAuthServices
    {
        private readonly UserManager<Account> _userManager;
        private readonly IConfiguration _configuration;
        private Account account;

        public AuthServices(UserManager<Account> userManager, IConfiguration configuration)
        {
            _userManager = userManager;
            _configuration = configuration;
        }


        public async Task<string> CreateToken()
        {
            var signingCredentials = GetSigningCredentials();
            var claims = await GetClaims();
            var tokenOptions = GenerateTokenOptions(signingCredentials, claims);

            return new JwtSecurityTokenHandler().WriteToken(tokenOptions);
        }

        private JwtSecurityToken GenerateTokenOptions(SigningCredentials signingCredentials, List<Claim> claims)
        {
            var jwtSettings = _configuration.GetSection("Jwt");
            var expiration = DateTime.Now.AddMinutes(30);

            var token = new JwtSecurityToken(
                issuer: jwtSettings.GetSection("Issuer").Value,
                claims: claims,
                expires: expiration,
                signingCredentials: signingCredentials
                );

            return token;
        }

        private async Task<List<Claim>> GetClaims()
        {
            var claims = new List<Claim>
            {
                new Claim(ClaimTypes.Name, account.UserName)
            };

            var roles = await _userManager.GetRolesAsync(account);

            foreach (var role in roles)
            {
                claims.Add(new Claim(ClaimTypes.Role, role));
            }

            return claims;
        }

        private SigningCredentials GetSigningCredentials()
        {
            var key = "OTUzMzNiM2UxOTQ1Y2Y2YTdjODhmM2NiZmQwZmNkNWM=";
            var sercet = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(key));

            return new SigningCredentials(sercet, SecurityAlgorithms.HmacSha256);
        }

        public async Task<bool> ValidateUser(LoginUserDTO loginUser)
        {
            account = await _userManager.FindByNameAsync(loginUser.Email);
            return account != null && await _userManager.CheckPasswordAsync(account, loginUser.Password);
        }
    }
}