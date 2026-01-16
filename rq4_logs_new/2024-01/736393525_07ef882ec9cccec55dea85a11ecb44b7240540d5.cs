using API.Models;
using BusinessLogic.Interfaces;
using BusinessLogic.Models;
using BusinessLogic.Services;
using Data.Entities;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.IdentityModel.Tokens;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Text;

namespace API.Controllers
{
    [ApiController]
    [Route("api/")]
    public class AuthenticationController
    {
        private readonly IAuthenticationService _authenticationService;
        private readonly IConfiguration _configuration;

        public AuthenticationController(IAuthenticationService authenticationService, IConfiguration configuration)
        {
            _authenticationService = authenticationService;
            _configuration = configuration;
        }

        // POST: api/signin
        [Route("signin")]
        [HttpPost]
        public async Task<ActionResult> LogIn([FromBody] UserCredentials userCredentials)
        {
            AuthenticationResult result;
            try
            {
                result = await _authenticationService.SignIn(userCredentials);
            }
            catch (Exception ex)
            {
                return new BadRequestObjectResult(ex.Message);
            }
            if (result.IsSuccessful)
            {
                return new OkObjectResult(GenerateJwtToken());
            }
            return new UnauthorizedObjectResult(result.ErrorMessage);
        }

        private Token GenerateJwtToken()
        {
            var tokenHandler = new JwtSecurityTokenHandler();
            var key = Encoding.ASCII.GetBytes(_configuration["Jwt:key"]);
            var expirationTime = DateTime.Now.AddDays(1);
            var tokenDescriptor = new SecurityTokenDescriptor
            {
                Subject = new ClaimsIdentity(),
                Expires = expirationTime,
                Issuer = _configuration["Jwt:Issuer"],
                Audience = _configuration["Jwt:Audience"],
                SigningCredentials = new SigningCredentials(new SymmetricSecurityKey(key), SecurityAlgorithms.HmacSha256Signature)
            };
            var token = tokenHandler.CreateToken(tokenDescriptor);
            return new Token 
            {
                AccessToken = tokenHandler.WriteToken(token),
                ExpiresIn = expirationTime
            };
        }

    }
}