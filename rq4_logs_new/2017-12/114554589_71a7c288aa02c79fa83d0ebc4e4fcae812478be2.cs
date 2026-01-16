using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;
using JWT.Model;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Tokens;

namespace JWT.Controllers
{
  [Route("api/[controller]")]
  public class TokenController : Controller
    {
    private IConfiguration _config;

    public TokenController(IConfiguration config)
    {
      _config = config;
    }

    [AllowAnonymous]
    [HttpPost]
    public async Task<IActionResult> CreateToken([FromBody]LoginModel login)
    {
      IActionResult response = Unauthorized();
      var user = await Authenticate(login);

      if (user != null)
      {
        var tokenString = BuildToken(user);
        response = Ok(new { token = tokenString });
      }

      return response;
    }

    private string BuildToken(UserModel user)
    {
      var claims = new[] {
        new Claim(JwtRegisteredClaimNames.Sub, user.Name),
        new Claim(JwtRegisteredClaimNames.Email, user.Email),
        new Claim(JwtRegisteredClaimNames.Website, user.Website),
        new Claim(JwtRegisteredClaimNames.Birthdate, user.Birthdate.ToString("yyyy-MM-dd")),
        new Claim(JwtRegisteredClaimNames.Jti, Guid.NewGuid().ToString())
    };
      var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(_config["Jwt:Key"]));
      var creds = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);

      var token = new JwtSecurityToken(_config["Jwt:Issuer"],
        _config["Jwt:Issuer"], claims,
        expires: DateTime.Now.AddMinutes(5),
        signingCredentials: creds);

      return new JwtSecurityTokenHandler().WriteToken(token);
    }

    private async Task<UserModel> Authenticate(LoginModel login)
    {
      var user = await GetUser(login);
      return user;
    }

    private async Task<UserModel> GetUser(LoginModel login)
    {
      return await Task.Run(() =>
       {
         UserModel user = null;
         if (login.Username == "test" && login.Password == "test")
         {
           user = new UserModel { Name = "Test Test", Email = "test.test@test.test", Website = "http://test.test", Birthdate = new DateTime(1900, 01, 01) };
         }
         else if (login.Username == "test2" && login.Password == "test2")
         {
           user = new UserModel { Name = "Test Test", Email = "test2.test2@test2.test2", Website = "http://test2.test2", Birthdate = new DateTime(1900, 01, 01) };
         }

         return user;
       });
    }
  }
}