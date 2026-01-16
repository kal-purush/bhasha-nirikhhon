using Project.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Project;

namespace _1.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class RegistrationController : ControllerBase
    {
        private readonly UserManager<ApplicationUser> _userManager;
        private readonly SignInManager<ApplicationUser> _signInManager;

        public RegistrationController(UserManager<ApplicationUser> userManager, SignInManager<ApplicationUser> signInManager)
        {
            _userManager = userManager;
            _signInManager = signInManager;
        }

        [HttpPost("Registration")]
        [ProducesResponseType(StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]

        public async Task<ActionResult> RegisterUserAsync(RegistrationDTO userDto)
        {
            if (userDto == null || string.IsNullOrWhiteSpace(userDto.Email) || string.IsNullOrEmpty(userDto.Password))
                return BadRequest();

            ApplicationUser newUser = new ApplicationUser
            {
                Email = userDto.Email,
                UserName = userDto.Email
            };

            var result = await _userManager.CreateAsync(newUser, userDto.Password);

            if (!result.Succeeded)
            {
                return BadRequest(result.Errors);
            }

            return Ok(result);
        }

        [HttpPost("Login")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status401Unauthorized)]

        public async Task<ActionResult> LoginUserAsync(LoginDTO userDto)
        {
            if (userDto == null || string.IsNullOrWhiteSpace(userDto.Email) || string.IsNullOrEmpty(userDto.Password))
                return BadRequest();

            var existingUser = await _userManager.FindByEmailAsync(userDto.Email);
            if (existingUser == null)
                return BadRequest();

            var result = await _signInManager.PasswordSignInAsync(existingUser, userDto.Password, true, false);

            if (!result.Succeeded)
            {
                return BadRequest();
            }

            return Ok("You Logged In succesfully");
        }

        [HttpPost("Logout")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public async Task<ActionResult> Logout()
        {
            await _signInManager.SignOutAsync();
            return Ok("You Logged Out Succesfully");
        }
    }
}