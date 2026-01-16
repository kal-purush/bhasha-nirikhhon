using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System.Security.Claims;

namespace PopApis.ApiControllers
{
    [Route("api/[controller]")]
    [ApiController]
    [Authorize(Policy = "User")]
    public class AuthenticationController : ControllerBase
    {
        [HttpGet]
        public string Login()
        {
            // We are here means login credentials are valid. Return user id
            return User.FindFirstValue(ClaimTypes.NameIdentifier);
        }
    }
}