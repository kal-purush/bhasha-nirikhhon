using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using GGsDB.Models;
using GGsLib;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace GGsAPI.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class UserController : ControllerBase
    {
        private readonly IUserService _userService;

        public UserController(IUserService userService)
        {
            _userService = userService;
        }
        [HttpGet("get")]
        [Produces("application/json")]
        public IActionResult GetAllUsers(int id)
        {
            try
            {
                return Ok(_userService.GetUserById(id));
            } catch (Exception)
            {
                return NotFound();
            }
        }
    }
}