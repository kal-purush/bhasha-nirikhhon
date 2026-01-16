using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using BookLib.Models.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Tokens;

// For more information on enabling MVC for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace BookLib.Controllers
{
    [Route("auth")]
    [ApiController]
    public class AuthenticateController : ControllerBase
    {
        public IConfiguration Configuration { get; }
        public AuthenticateController(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        [HttpPost("token", Name = nameof(GenerateToken))]
        public IActionResult GenerateToken(LoginUser loginUser)
        {
            if (loginUser.Username != "demouser" || loginUser.Password != "demopassword")
            {
                return Unauthorized();
            }

            var claims = new List<Claim>
            {
                new Claim(JwtRegisteredClaimNames.Sub, loginUser.Username)
            };

            var tokenConfigSection = Configuration.GetSection("Security:Token");
            var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(tokenConfigSection["Key"]));
            var sigCredential = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);

            //定义jwt token
            var jwtToken = new JwtSecurityToken(
                    issuer: tokenConfigSection["Issuer"],
                    audience: tokenConfigSection["Audience"],
                    claims: claims,
                    expires: DateTime.Now.AddMinutes(3),
                    signingCredentials: sigCredential);
            return Ok(new {
                //通过JwtSecurityTokenHandler()的WriteToken方法生成jwt token
                token = new JwtSecurityTokenHandler().WriteToken(jwtToken),
                expiration = TimeZoneInfo.ConvertTimeFromUtc(jwtToken.ValidTo, TimeZoneInfo.Local)
            });

        }
    }
}