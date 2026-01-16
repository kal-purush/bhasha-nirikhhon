using Domain.Entities;
using Microsoft.IdentityModel.Tokens;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;

namespace API.Authentication
{
    public class MockJwtTokens
    {
        public static string GenerateJwtToken(Member user)
        {
            var securityKey = new SymmetricSecurityKey(System.Text.Encoding.Default.GetBytes("S0M3RAN0MS3CR3T!1!MAG1C!1!343456y674688847"));
            var credentials = new SigningCredentials(securityKey, SecurityAlgorithms.HmacSha256);

            var claims = new[]
            {
                new Claim(JwtRegisteredClaimNames.Sub, user.Id.ToString()),
                new Claim(ClaimTypes.Name, user.Name),
                new Claim(ClaimTypes.NameIdentifier, user.ChandaNo),
                new Claim(JwtRegisteredClaimNames.Email, user.Email),
                new Claim(ClaimTypes.GroupSid, user.JamaatId.ToString()),
                new Claim(ClaimTypes.Role, String.Join(",",user.MemberRoles.Select(m => m.RoleName).ToArray())),
                new Claim(JwtRegisteredClaimNames.Jti, Guid.NewGuid().ToString()),
            };

            var allClaims = new List<Claim>(claims);

            var token = new JwtSecurityToken(
                issuer: "https://your-keycloak-domain/auth/realms/your-realm",
                audience: "your-client-id",
                claims: allClaims,
                expires: DateTime.Now.AddHours(1),
                signingCredentials: credentials);

            return new JwtSecurityTokenHandler().WriteToken(token);
        }
    }
}