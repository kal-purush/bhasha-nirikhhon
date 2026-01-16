using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Monitoring.Models;
using Monitoring.Models.DTOs;

namespace Monitoring.Controllers
{
    [Authorize]
    [Route("api/account")]
    [ApiController]
    public class AccountController : ControllerBase
    {
        private readonly UserManager<Client> _userManager;

        public AccountController(UserManager<Client> userManager)
        {
            _userManager = userManager;
        }

        // ✅ Get User Profile
        [HttpGet("profile")]
        public async Task<IActionResult> GetProfile()
        {
            var user = await _userManager.GetUserAsync(User);
            if (user == null) return NotFound();

            return Ok(new { user.Name, user.Email});
        }

        // ✅ Update Profile
        [HttpPut("profile")]
        public async Task<IActionResult> UpdateProfile([FromBody] UpdateProfileDto model)
        {
            var user = await _userManager.GetUserAsync(User);
            if (user == null) return NotFound();

            user.Name = model.Name;

            await _userManager.UpdateAsync(user);
            return Ok(new { message = "Profile updated" });
        }
    }
}