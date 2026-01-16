using System.Threading.Tasks;
using Battleships.Data.Dto;
using Battleships.Services.Authentication.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace Battleships.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class AuthenticationController : ControllerBase
    {
        private readonly IAuthenticationService _authenticationService;

        public AuthenticationController(IAuthenticationService authenticationService)
        {
            _authenticationService = authenticationService;
        }

        [HttpPost("login")]
        public async Task<IActionResult> Login([FromBody] UserCredentialsDto userCredentialsDto)
        {
            var token = await _authenticationService.Login(userCredentialsDto);

            return Ok(new { token });
        }

        [HttpPost("register")]
        public async Task<IActionResult> Register([FromBody] UserCredentialsDto registration)
        {
            await _authenticationService.Register(registration);

            return Ok();
        }
    }
}