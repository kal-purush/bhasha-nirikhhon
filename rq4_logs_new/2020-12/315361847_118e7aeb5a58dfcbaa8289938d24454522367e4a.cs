using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;
using BookLib.Entities;
using BookLib.Models.Authorization;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Tokens;


namespace BookLib.Controllers
{
    //此controller是为了校验用户登录信息并且生成token
    [Route("auth")]
    [ApiController]
    public class AuthenticateController : ControllerBase
    {
        public IConfiguration Configuration { get; }
        public UserManager<User> UserManager { get; }
        public RoleManager<Role> RoleManager { get; }
        public AuthenticateController(
                    IConfiguration configuration,
                    UserManager<User> userManager,
                    RoleManager<Role> roleManager)
        {
            Configuration = configuration;
            UserManager = userManager;
            RoleManager = roleManager;
        }

        [HttpPost("register", Name = nameof(AddUserAsync))]
        public async Task<IActionResult> AddUserAsync (RegisterUser registerUser)
        {
            var user = new User
            {
                UserName = registerUser.UserName,
                Email = registerUser.Email,
                BirthDate = registerUser.BirthDate
            };

            IdentityResult result = await UserManager.CreateAsync(user, registerUser.Password);
            if (result.Succeeded)
            {
                return Ok();
            }
            else
            {
                ModelState.AddModelError("Error", result.Errors.FirstOrDefault()?.Description);
                return BadRequest(ModelState);
            }
        }

        [HttpPost("token2", Name = nameof(GenerateTokenAsync))]
        [EnableCors("AllowAllMethodsPolicy")]
        public async Task<IActionResult> GenerateTokenAsync(LoginUser loginUser)
        {
            //验证用户账号和密码是否正确
            var user = await UserManager.FindByNameAsync(loginUser.Username);
            if(user == null)
            {
                Console.WriteLine("user cannot find");
                return Unauthorized();
            }

            var result = UserManager.PasswordHasher.VerifyHashedPassword(user, user.PasswordHash, loginUser.Password);
            if(result != PasswordVerificationResult.Success)
            {
                Console.WriteLine("password is not correct");
                return Unauthorized();
            }

            //获取用户claims和role
            var userClaims = await UserManager.GetClaimsAsync(user);
            var userRoles = await UserManager.GetRolesAsync(user);
            foreach (var roleItem in userRoles)
            {
                userClaims.Add(new Claim(ClaimTypes.Role, roleItem));
            }

            //将用户claims和role包含到token中
            var claims = new List<Claim>
            {
                new Claim(JwtRegisteredClaimNames.Sub, user.UserName),
                new Claim(JwtRegisteredClaimNames.Jti, Guid.NewGuid().ToString()),
                new Claim(JwtRegisteredClaimNames.Email, user.Email)
            };
            claims.AddRange(claims);

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
            return Ok(new
            {
                //通过JwtSecurityTokenHandler()的WriteToken方法生成jwt token
                token = new JwtSecurityTokenHandler().WriteToken(jwtToken),
                expiration = TimeZoneInfo.ConvertTimeFromUtc(jwtToken.ValidTo, TimeZoneInfo.Local)
            });
        }

        [HttpPost("token", Name = nameof(GenerateToken))]
        [ApiExplorerSettings(IgnoreApi = true)] //不在Swagger文档中展示
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