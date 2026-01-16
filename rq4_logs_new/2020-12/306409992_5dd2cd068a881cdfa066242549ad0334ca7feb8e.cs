using Backend;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using Service.UsersServices;
using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Security.Claims;
using System.Text;
using System.Threading.Tasks;
using WebAppPatient.Dto;

namespace WebAppPatient.Controllers
{
    [Authorize]
    [ApiController]
    [Route("api/[controller]")]
    public class UserController : ControllerBase
    {     
        private readonly AppSettings _appSettings;
        public UserController(IOptions<AppSettings> appSettings)
        {
            _appSettings = appSettings.Value;
        }

        [AllowAnonymous]
        [HttpPost("authenticate")]
        public IActionResult Authenticate([FromBody] AuthenticateDto model)
        {          
            var user = App.Instance().UserService.Authenticate(model.Username, model.Password, Encoding.ASCII.GetBytes(_appSettings.Secret));
            if (user == null)
            {
                return Forbid();
            }
            return Ok(user);
        }
    }
}