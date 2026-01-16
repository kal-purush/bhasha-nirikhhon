using System.Security.Claims;
using Domain;
using Infrastructure.EF.JwtAuthentication;
using Microsoft.AspNetCore.Mvc;

namespace WebApiTroc.Controllers;

[Route("api/[controller]")]
[ApiController]
public class AuthenticationController : ControllerBase
{
    private readonly IJwtAuthenticationService _jwtAuthenticationService;
    private readonly IConfiguration _config;
   
    public AuthenticationController(IJwtAuthenticationService jwtAuthenticationService, IConfiguration config)
    {
        _jwtAuthenticationService = jwtAuthenticationService;
        _config = config;
    }
   
    [HttpPost]
    [Route("login")]
    public IActionResult Login([FromBody] LoginModel model)
    {
        var user = _jwtAuthenticationService.Authenticate(model.Email, model.Mdp);

        if (user != null)
        {
            var claims = new List<Claim>
            {
                new Claim(ClaimTypes.Email, user.Email)
            
            };
            var token = _jwtAuthenticationService.GenerateToken(_config["Jwt:Key"], claims);
            return Ok(token);
        }

        return Unauthorized();
    }

}