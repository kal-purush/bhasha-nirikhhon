using API.Middlewares.Exceptions;
using API.Services.Token;
using Application.Models.Authentication;
using Application.Services.AuthenticationServices;
using Application.ViewModels.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AuthenticationController : ControllerBase
    {
        private readonly IGenerateJwtToken _generateJwtToken;
        private readonly IAuthService _authService;

        public AuthenticationController(IGenerateJwtToken generateJwtToken, IAuthService authService)
        {
            _generateJwtToken = generateJwtToken;
            _authService = authService;
        }
        
        [HttpPost("[action]")]
        [AllowAnonymous]
        public ActionResult<AuthenticationResponseModel> Login([FromBody] LoginRequestModel user)
        {
            var auth = _generateJwtToken.GenerateToken(user).Result;
            if (auth is null)
            {
                NotFoundException.Throw("006", "Usuário e/ou senha incorretos.");   
            }
            
            return Ok(auth);
        }
        
        [HttpPost("[action]")]
        [Authorize(Roles = "Admin")]
        public ActionResult<AuthenticationResponseModel> Register([FromBody] RegisterModel user)
        {
            var result = _authService.Register(user);
            return Ok(result);
        }
    }
}