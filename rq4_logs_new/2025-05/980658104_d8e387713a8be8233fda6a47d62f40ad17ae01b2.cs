using LocalAccountServiceProvider.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace LocalAccountServiceProvider.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class UsersController(IAppUserService appUserService) : ControllerBase
    {
        private readonly IAppUserService _appUserService = appUserService;

        [HttpGet]
        public async Task<IActionResult> GetAllUsers()
        {
            var users = await _appUserService.GetAllAppUsers();

            if (users == null)
            {
                return NotFound();
            }
            return Ok(users);
        }

        [HttpGet("{id}")]
        public async Task<IActionResult> GetUser(string id)
        {
            var user = await _appUserService.GetAppUserById(id);
            if (user == null)
            {
                return NotFound();
            }
            return Ok(user);
        }
    }
}