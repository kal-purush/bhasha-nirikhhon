
using BlinkIt.Gateway.Models;
using BlinkIt.Service.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace BlinkIt.Gateway.Controllers;

[ApiController]
[Route("api/[controller]")]
public class AuthController : ControllerBase
{
    private readonly IAuthService _authService;
    public AuthController(IAuthService authService)
    {
        _authService = authService;
    }
    [HttpPost("signup")]
    public async Task<IActionResult> SignUp([FromBody] LoginRequest loginRequest)
    {
        if (loginRequest == null || string.IsNullOrWhiteSpace(loginRequest.MobileNumber) || string.IsNullOrWhiteSpace(loginRequest.Password))
        {
            return BadRequest("Mobile number and password are required.");
        }

        var (success, message, token) = await _authService.CreateNewUser(loginRequest.MobileNumber, loginRequest.Password);

        if (success)
        {
            return Ok(new { Message = message, Token = token });
        }
        else if (message == "Mobile number already exists. Cannot create new User.")
        {
            return BadRequest(message);
        }
        return Unauthorized(new { Message = message });
    }
}