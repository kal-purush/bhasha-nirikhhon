using Event_Planinng_System_DAL.Models;
using Event_Planning_System.IServices;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Event_Planning_System.DTO;

namespace Event_Planning_System.Controllers
{
    [ApiController]
    [Route("api/[controller]")]

    public class LoginController : Controller
    {
        private readonly UserManager<User> _userManager;
        private readonly SignInManager<User> _signInManager;
        private readonly IaccountServices _accountservices;
        public LoginController(UserManager<User> userManager, SignInManager<User> signInManager, IaccountServices tokenService)
        {
            _userManager = userManager;
            _signInManager = signInManager;
            _accountservices = tokenService;
        }
        [HttpPost("login")]
        public async Task<IActionResult> Login([FromBody] LoginDto model)
        {
            var user = await _userManager.FindByEmailAsync(model.Email);
            if (user != null && await _userManager.CheckPasswordAsync(user, model.Password))
            {
                var token = await _accountservices.GenerateToken(user);
                return Ok(new { token });
            }
            return Unauthorized();
        }
    }
}