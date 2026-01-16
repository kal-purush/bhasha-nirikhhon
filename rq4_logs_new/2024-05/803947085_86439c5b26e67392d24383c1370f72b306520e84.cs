using Event_Planning_System.DTO.Mail;
using Event_Planning_System.IServices;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace Event_Planning_System.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AuthController : ControllerBase

    {
        private readonly IAuthService authService;

        public AuthController(IAuthService authService)
        {
            this.authService = authService;
        }

        [HttpPost]
        [Route("emailconfirm")]
        public async Task<IActionResult> EmailConfirm([FromQuery] ConfirmEmailDto confirmEmailDto)
        {
            var user = await  authService.GetUserByEmail(confirmEmailDto.Email);
            if (user == null)
            {
                return BadRequest("User not found");
            }
            var result = await authService.ValidateEmailToken(user,confirmEmailDto.Token);
            if (result.Succeeded)
            {
                return Ok("Email confirmed");
            }
            return BadRequest("Email not confirmed");
        }
    }
}