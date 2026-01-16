using System;
using System.Linq;
using System.Text;
using Microsoft.Extensions.Configuration;
using System.Security.Claims;
using System.IdentityModel.Tokens.Jwt;
using Microsoft.IdentityModel.Tokens;
using System.Security.Cryptography;

namespace OnlineMarks.Tools.Auth
{
    public class AuthManager : IAuthManager
    {

        private readonly IConfiguration _configuration;

        public AuthManager(IConfiguration configuration){
            _configuration = configuration;
        }


        public string GenerateToken(String id, String role){
            var tokenHandler = new JwtSecurityTokenHandler();
            var key = Encoding.ASCII.GetBytes(_configuration["AppSettings:Secret"]);
            var tokenDescriptor = new SecurityTokenDescriptor
            {
                Subject = new ClaimsIdentity(new Claim[]
                {
                  new Claim(ClaimTypes.Name, id),
                  new Claim(ClaimTypes.Role, role)
                }),
                Expires = DateTime.UtcNow.AddDays(30),
                SigningCredentials = new SigningCredentials(new SymmetricSecurityKey(key), SecurityAlgorithms.HmacSha256Signature)
            };
            var token = tokenHandler.CreateToken(tokenDescriptor);

            return tokenHandler.WriteToken(token);
        }
        public (byte[], byte[]) CreatePasswordHashAndSalt(string password){

            var hmac = new HMACSHA512();
            var passwordSalt = hmac.Key;
            var passwordHash = hmac.ComputeHash(Encoding.UTF8.GetBytes(password));

            return (passwordHash, passwordSalt);
        }

        public bool VerifyPaswordHash(string password, byte[] salt, byte[] passwordHash){
            
            var hash = new HMACSHA512(salt);
            byte[] calcPassHash = hash.ComputeHash(Encoding.UTF8.GetBytes(password));

            return calcPassHash.SequenceEqual(passwordHash) ? true : false;
        }
    }
}