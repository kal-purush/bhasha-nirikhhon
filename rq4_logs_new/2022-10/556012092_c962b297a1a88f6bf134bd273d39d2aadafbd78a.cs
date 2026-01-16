using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;
using AgendaApi.Data.Repository.Interfaces;
using AgendaApi.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.IdentityModel.Tokens;

namespace AgendaApi.Controllers;

[Route("api/authentication")]
[ApiController]
public class AuthenticationController : ControllerBase
{
    private readonly IConfiguration _config;
    private readonly IUserRepository _userRepository;

    public AuthenticationController(IConfiguration config, IUserRepository userRepository)
    {
        _config = config;
        _userRepository = userRepository;
    }

    [HttpPost("authenticate")]
    public ActionResult<string> Autenticar(AuthenticationRequestBody authenticationRequestBody)
    {
        var user = _userRepository.ValidateUser(authenticationRequestBody);

        if (user is null)
            return Unauthorized();

        var securityPassword =
            new SymmetricSecurityKey(
                Encoding.ASCII.GetBytes(
                    _config[
                        "Authentication:SecretForKey"]));

        var credentials = new SigningCredentials(securityPassword, SecurityAlgorithms.HmacSha256);

        var claimsForToken = new List<Claim>
        {
            new Claim("sub", user.Id.ToString()),
            new Claim("given_name", user.FirstName),
            new Claim("family_name", user.LastName)
        };

        var jwtSecurityToken = new JwtSecurityToken(
            _config["Authentication:Issuer"],
            _config["Authentication:Audience"],
            claimsForToken,
            DateTime.UtcNow,
            DateTime.UtcNow.AddHours(1),
            credentials);

        var tokenToReturn = new JwtSecurityTokenHandler()
            .WriteToken(jwtSecurityToken);

        return Ok(tokenToReturn);
    }
}