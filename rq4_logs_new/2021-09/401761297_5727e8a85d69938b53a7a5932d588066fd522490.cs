using Disney.Application.AuthCommands;
using Disney.Application.Extensions;
using Disney.Application.Interfaces;
using Disney.Domain.DTOs;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Disney.API.Controllers
{
    [Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]
    [Route("api/[controller]")]
    [ApiController]
    public class UsersController : ControllerBase
    {
        #region Object and Constructor

        private readonly IUsersServices usersServices;
        public UsersController(IUsersServices usersServices)
        {
            this.usersServices = usersServices;
        }
        #endregion

        [HttpGet]
        [Authorize]
        public async Task<ActionResult<IEnumerable<UserDTO>>> GetAllUsers()
        {
            var response = usersServices.GetAllUsers();

            return Ok(await response);
        }

        [HttpPost]
        [Authorize]
        [AllowAnonymous]
        public async Task<IActionResult> RegisterUser([FromBody] RegisterUser request)
        {
            var response = await usersServices.RegisterUser(request);

            return (response.HasErrors)
                ? BadRequest(response.Messages)
                : Ok(response);
        }

        [HttpPost]
        [Authorize]
        [AllowAnonymous]
        public async Task<IActionResult> LoginUser([FromBody] LoginUser request)
        {
            var response = await usersServices.LoginUser(request);

            return response.HasErrors
                ? BadRequest(response.Messages)
                : Ok(response);
        }

        [HttpDelete]
        [Authorize]
        [AllowAnonymous]
        public async Task<IActionResult> DeleteUser([FromRoute] DeleteUser request)
        {
            request.SetUser(User.GetUserId());
            var response = await usersServices.DeleteUser(request);

            return response.HasErrors
                ? BadRequest(response.Messages)
                : Ok(response);
        }
    }
}