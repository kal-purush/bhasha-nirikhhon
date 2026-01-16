using GryGiereczki.Models;
using GryGiereczki.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace GryGiereczki.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class RegisterController : ControllerBase
    {
        
        public RegisterService _RegisterService;

        public RegisterController(RegisterService registerService)
        {
            _RegisterService = registerService;
        }
        
        [HttpGet] // zrobilem tylko zeby przetestować wyświatlnie
        public IActionResult Test()
        {
             var test = _RegisterService.Test();
            return  Ok(test);
        }

        [HttpPost]
        public IActionResult Register(User user)
        {
            _RegisterService.Register(user);
            return Ok();
        }
        

    }
}