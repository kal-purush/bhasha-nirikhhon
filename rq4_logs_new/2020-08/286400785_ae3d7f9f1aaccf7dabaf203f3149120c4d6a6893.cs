using Microsoft.AspNetCore.Mvc;
using OnlineMarks.Interfaces.Services;
using System;
using System.Linq;

namespace OnlineMarks.Controllers
{
    [Route("/user")]
    [ApiController]
    public class UserController : ControllerBase
    {
        IUserService _userService;

        public UserController(IUserService userService)
        {
            _userService = userService;
        }

        [HttpGet]
        public IActionResult GetAll()
        {
            //var users = _userService.GetAll();
            return Ok("Pogodili ste GetAll");
        }
    }
}