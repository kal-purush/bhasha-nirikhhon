using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using WebAPI.Identity;

namespace WebAPI.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class AuthController : ControllerBase
    {
        [HttpPost]
        [AllowAnonymous]
        public IActionResult Authenticate([FromBody] AuthModel model)
        {
            if(model.UserName == "test" && model.Password == "123123")
            {
                return Ok(TokenService.GenerateToken(model.UserName));

            }

            return BadRequest("Kullanıcı adı veya şifre hatalı.");
        }
    }


    public class AuthModel
    {
        public string UserName { get; set; }
        public string Password { get; set; }
    }
}