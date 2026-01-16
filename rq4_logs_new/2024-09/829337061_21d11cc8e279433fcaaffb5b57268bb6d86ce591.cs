using Microsoft.IdentityModel.Tokens;
using System.Data;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;

namespace IdentityServiceAPI.Services
{
    public class AuthorizationService(ILogger<AuthorizationService> logger, IIdentityService identityService, IConfiguration configuration) : IAuthorizationService
    {
        public async Task<string> GetJwtTokenAsync(string userName)
        {
            var user = await identityService.FindUserByUserName(userName)
                ?? throw new Exception("User not found");

            var userRoles = await identityService.GetUserRoles(user);

            var claims = new List<Claim>()
            {
                new("Id", user.Id.ToString()),
                new(JwtRegisteredClaimNames.Sub, user.UserName!),
                new(JwtRegisteredClaimNames.Jti, Guid.NewGuid().ToString()),
            };

            claims.AddRange(userRoles.Select(x =>
                new Claim(ClaimsIdentity.DefaultRoleClaimType, x)));

            var config = configuration.GetSection("JwtToken");
            var issuer = config["Issuer"]
                ?? throw new KeyNotFoundException("JWT Configuration issuer not found!");
            var audience = config["Audience"]
                ?? throw new KeyNotFoundException("JWT Configuration audience not found!");
            var key = config["Key"]
                ?? throw new KeyNotFoundException("JWT Configuration key not found!");
            var timeout = config["TimeoutInMinutes"]
                ?? throw new KeyNotFoundException("JWT Configuration timeout not found!");

            var tokenDescriptor = new SecurityTokenDescriptor
            {
                Subject = new ClaimsIdentity(claims),
                Expires = DateTime.UtcNow.AddMinutes(int.Parse(timeout)),
                Issuer = issuer,
                Audience = audience,
                SigningCredentials = new SigningCredentials(new SymmetricSecurityKey(Encoding.ASCII.GetBytes(key)), SecurityAlgorithms.HmacSha256)
            };

            var tokenHandler = new JwtSecurityTokenHandler();
            var token = tokenHandler.CreateToken(tokenDescriptor);
            var tokenToString = tokenHandler.WriteToken(token);

            logger.LogInformation("JWT Token for user {} (ID: {}) created", user.UserName, user.Id);

            return tokenToString;
        }
    }
}