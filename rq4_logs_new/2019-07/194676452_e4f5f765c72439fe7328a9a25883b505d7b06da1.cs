using Microsoft.AspNetCore.Mvc;
using ScheduleSystem.DataApi.Base;
using ScheduleSystem.DataApi.Controllers.User.ViewModels;

namespace ScheduleSystem.DataApi.Controllers.User
{
    [Route("users")]
    public class UserController : ApiControllerBase
    {
        [HttpPost]
        [Route("login")]
        public IActionResult SignIn([FromBody] UserViewModel userViewModel)
        {
            return Ok(userViewModel);
        }

        [HttpPost]
        [Route("registration")]
        public IActionResult SignUp([FromBody] UserViewModel userViewModel)
        {
            return Ok(userViewModel);
        }
    }
}