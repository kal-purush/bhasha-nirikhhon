using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Tokens;
using MyAPI.Constants;
using MyAPI.DTOs.User;
using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;

namespace MyAPI.Controllers
{
    [Route("api/tai-khoan")]
    [ApiController]
    public class AccountController : ControllerBase
    {
        private readonly UserManager<IdentityUser> _userManager;
        private readonly SignInManager<IdentityUser> _signInManager;
        private readonly IConfiguration _configuration;

        public AccountController(UserManager<IdentityUser> userManager, SignInManager<IdentityUser> signInManager,
            IConfiguration configuration)
        {
            _userManager = userManager;
            _signInManager = signInManager;
            _configuration = configuration;
        }

        [HttpPost("register")]
        public async Task<ActionResult<AuthenResponse>> Register([FromBody] UserCredsRequest userCreds)
        {
            var user = new IdentityUser()
            {
                UserName = userCreds.Email,
                Email = userCreds.Email
            };

            var result = await _userManager.CreateAsync(user, userCreds.Pass);

            if (result.Succeeded)
            {
                return BuildToken(userCreds);
            }
            else
            {
                return BadRequest(result.Errors);
            }
        }

        [HttpPost("login")]
        public async Task<ActionResult<AuthenResponse>> Login([FromBody] UserCredsRequest userCreds)
        {
            var result =
                await _signInManager.PasswordSignInAsync(userCreds.Email, userCreds.Pass, false, false);

            if (result.Succeeded)
            {
                return BuildToken(userCreds);
            }
            else
            {
                return BadRequest("Đăng nhập không hợp lệ");
            }
        }

        private AuthenResponse BuildToken(UserCredsRequest userCreds)
        {
            var claims = new List<Claim>()
            {
                new Claim(type: "email", value: userCreds.Email)
            };

            var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(_configuration[SystemConstants.Key]));
            var creds = new SigningCredentials(key: key, algorithm: SecurityAlgorithms.HmacSha256);
            var exp = DateTime.UtcNow.AddDays(1);
            var token =
                new JwtSecurityToken(issuer: null, audience: null, claims: claims, expires: exp, signingCredentials: creds);

            return new AuthenResponse()
            {
                Expiration = exp,
                Token = new JwtSecurityTokenHandler().WriteToken(token)
            };
        }
    }
}