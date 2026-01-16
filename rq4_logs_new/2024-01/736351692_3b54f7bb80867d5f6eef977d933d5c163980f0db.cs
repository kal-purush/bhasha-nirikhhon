using System;
using Microsoft.AspNetCore.Authorization;
using System.Data;
using Microsoft.AspNetCore.Mvc;

namespace home_swap_api.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class TestRoles : ControllerBase
    {
        public TestRoles()
        {
        }

        [HttpGet("user-route"), Authorize(Roles = "User")]
        public ActionResult<string> UserRoute()
        {
            return Ok("This is user route!");
        }

        [HttpGet("admin-route"), Authorize(Roles = "Admin")]
        public ActionResult<string> AdminRoute()
        {
            return Ok("This is admin route!");

        }
    }
}
