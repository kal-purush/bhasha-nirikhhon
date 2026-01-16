using Microsoft.AspNetCore.Mvc;
using System;
using System.Threading.Tasks;

namespace MAJServices.Controllers
{
    public class AccountController : Controller
    {
        //[Route("Create")]
        //[HttpPost]
        //public async Task<IActionResult> CreateUser([FromBody] UserInfo model)
        //{
        //    if (ModelState.IsValid)
        //    {
        //        var user = new ApplicationUser { UserName = model.Email, Email = model.Email };
        //        var result = await _userManager.CreateAsync(user, model.Password);
        //        if (result.Succeeded)
        //        {
        //            return BuildToken(model);
        //        }
        //        else
        //        {
        //            return BadRequest("Username or password invalid");
        //        }
        //    }
        //    else
        //    {
        //        return BadRequest(ModelState);
        //    }

        //}

        //[HttpPost]
        //[Route("Login")]
        //public async Task<IActionResult> Login([FromBody] UserInfo userInfo)
        //{
        //    if (ModelState.IsValid)
        //    {
        //        var result = await _signInManager.PasswordSignInAsync(userInfo.Email, userInfo.Password, isPersistent: false, lockoutOnFailure: false);
        //        if (result.Succeeded)
        //        {
        //            return BuildToken(userInfo);
        //        }
        //        else
        //        {
        //            ModelState.AddModelError(string.Empty, "Invalid login attempt.");
        //            return BadRequest(ModelState);
        //        }
        //    }
        //    else
        //    {
        //        return BadRequest(ModelState);
        //    }
        //}

        //private IActionResult BuildToken(UserInfo userInfo)
        //{
        //    var claims = new[]
        //    {
        //        new Claim(JwtRegisteredClaimNames.UniqueName, userInfo.Email),
        //        new Claim("miValor", "Lo que yo quiera"),
        //        new Claim(JwtRegisteredClaimNames.Jti, Guid.NewGuid().ToString())
        //    };

        //    var key = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(_configuration["Llave_super_secreta"]));
        //    var creds = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);

        //    var expiration = DateTime.UtcNow.AddHours(1);

        //    JwtSecurityToken token = new JwtSecurityToken(
        //       issuer: "yourdomain.com",
        //       audience: "yourdomain.com",
        //       claims: claims,
        //       expires: expiration,
        //       signingCredentials: creds);

        //    return Ok(new
        //    {
        //        token = new JwtSecurityTokenHandler().WriteToken(token),
        //        expiration = expiration
        //    });

        //}
    }
}