using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using Microsoft.IdentityModel.Tokens;
using Microsoft.Extensions.Configuration;
using Microsoft.AspNetCore.Http;
using System.Security.Cryptography;

namespace YouCode.GUI.Services.Auth
{
    public class AuthenticationService
    {
        public static string GenerateJwtToken(string username)
        {
            var configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .Build();

            var xd = "E4C54F0E688584D8D57F1A9C238A6B3A71EAD74F3B6F6B9AB4B936059E44C4BD";

            var sectionName = "JwtSettings";
            var key = Encoding.UTF8.GetBytes(xd);
            var tokenHandler = new JwtSecurityTokenHandler();
            var tokenDescriptor = new SecurityTokenDescriptor
            {
                Subject = new ClaimsIdentity(new Claim[]
                {
                    new Claim(ClaimTypes.Name, username)
                }),
                Expires = DateTime.Now.AddMonths(1),
                SigningCredentials = new SigningCredentials(new SymmetricSecurityKey(key), SecurityAlgorithms.HmacSha256Signature)
            };
            var token = tokenHandler.CreateToken(tokenDescriptor);
            var tokenString = tokenHandler.WriteToken(token);
            
            return tokenString;

        }

        
        public static string ValidateToken(string token)
        {
            var configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .Build();
            
             var xd = "E4C54F0E688584D8D57F1A9C238A6B3A71EAD74F3B6F6B9AB4B936059E44C4BD";
            var sectionName = "JwtSettings";
            var key = xd;
            if (token == null)
                return null;

            var tokenHandler = new JwtSecurityTokenHandler();
            var keyBytes = Encoding.UTF8.GetBytes(key);

            try
            {
                tokenHandler.ValidateToken(token, new TokenValidationParameters
                {
                    ValidateIssuerSigningKey = true,
                    IssuerSigningKey = new SymmetricSecurityKey(keyBytes),
                    ValidateIssuer = false,
                    ValidateAudience = false,
                    ClockSkew = TimeSpan.Zero
                }, out SecurityToken validatedToken);

                var jwtToken = (JwtSecurityToken)validatedToken;
                var userName = jwtToken.Claims.FirstOrDefault(claim => claim.Type == "unique_name")?.Value;

                return userName;
            }
            catch
            {
                return null;
            }
        }


        public static string EncryptToken(string token)
        {
            var xd = "w54D%k2Tf!Lp#Z@1^X8~j7*qY&v9Nc$E";
            var key = Encoding.ASCII.GetBytes(xd);
            using (var aesAlg = Aes.Create())
            {
                aesAlg.Key = key;
                aesAlg.IV = new byte[16]; 

                var encryptor = aesAlg.CreateEncryptor(aesAlg.Key, aesAlg.IV);
                var tokenBytes = Encoding.UTF8.GetBytes(token);

                byte[] encryptedToken;
                using (var msEncrypt = new System.IO.MemoryStream())
                {
                    using (var csEncrypt = new System.Security.Cryptography.CryptoStream(msEncrypt, encryptor, System.Security.Cryptography.CryptoStreamMode.Write))
                    {
                        csEncrypt.Write(tokenBytes, 0, tokenBytes.Length);
                        csEncrypt.FlushFinalBlock();
                        encryptedToken = msEncrypt.ToArray();
                    }
                }
                return Convert.ToBase64String(encryptedToken);
            }
        }

        public static string DecryptToken(string encryptedToken)
        {
            var xd = "w54D%k2Tf!Lp#Z@1^X8~j7*qY&v9Nc$E";
            var key = Encoding.ASCII.GetBytes(xd);
            using (var aesAlg = Aes.Create())
            {
                aesAlg.Key = key;
                aesAlg.IV = new byte[16];

                var decryptor = aesAlg.CreateDecryptor(aesAlg.Key, aesAlg.IV);
                var encryptedTokenBytes = Convert.FromBase64String(encryptedToken);

                byte[] tokenBytes;
                using (var msDecrypt = new System.IO.MemoryStream(encryptedTokenBytes))
                {
                    using (var csDecrypt = new System.Security.Cryptography.CryptoStream(msDecrypt, decryptor, System.Security.Cryptography.CryptoStreamMode.Read))
                    {
                        using (var srDecrypt = new System.IO.StreamReader(csDecrypt))
                        {
                            tokenBytes = Encoding.UTF8.GetBytes(srDecrypt.ReadToEnd());
                        }
                    }
                }
                return Encoding.UTF8.GetString(tokenBytes);
            }
        }
    


        public static bool IsUserLogged(HttpContext httpContext)
        {
            if (httpContext == null)
                return false;

            var token = httpContext.Session.GetString("JwtToken");

            return !string.IsNullOrEmpty(token);
        }

        public static int GetUserId(HttpContext httpContext)
        {
            if (httpContext == null)
                return 0;

            var id = int.Parse(httpContext.Session.GetString("UserID"));

            if (id == null){
                return 0;
            }
            else{
                return id;
            }
        }

        public static string GetUserName(HttpContext httpContext)
        {
            if (httpContext == null)
                return null;

            var username = httpContext.Session.GetString("UserName");
            
            if (string.IsNullOrEmpty(username)){
                return null;
            }
            else
            {
                return username;
            }
        }

    }
}