using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using YourScheduler.BusinessLogic.Models;
using YourScheduler.Infrastructure;
using YourScheduler.Infrastructure.Entities;

namespace YourScheduler.WebApplication.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AccountController : ControllerBase
    {
        private readonly YourSchedulerDbContext _context;
        private readonly SignInManager<ApplicationUser> _signInManager;

        public AccountController(YourSchedulerDbContext context, SignInManager<ApplicationUser> signInManager)
        {
            _context = context;
            _signInManager = signInManager;
        }

        [HttpPost]
        [Route("signIn")]
        public async Task<IActionResult> SignIn([FromBody] AuthorizationModel model)
        {
            var result = await _signInManager.PasswordSignInAsync(model.Email, model.Password, true, false);
            return Ok(result);
        }
    }
}