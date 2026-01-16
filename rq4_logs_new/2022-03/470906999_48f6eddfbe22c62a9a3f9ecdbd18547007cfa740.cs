
using Assessment.Common.Models.Request;
using Common.Authorization;
using Common.Helpers.Services;

using Microsoft.AspNetCore.Mvc;


namespace SignUpApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class UsersController : ControllerBase
    {
        private ISignUpService _signUpService;
        public UsersController(ISignUpService signUpService)
        {
            _signUpService = signUpService;
        }


        

        [HttpPost]
        [Route("register")]
        public IActionResult Create([FromBody] SignUpRequest model)
        {
            _signUpService.CreateUser(model);
          
            return Ok(new { message = "User created" });
        }

        [AllowAnonymous]
        [HttpPost]
        [Route("login")]
        public IActionResult Login([FromBody] LoginRequest model)
        {
            var user = _signUpService.Login(model);

            if (user == null)
                return BadRequest(new { message = "Username or password is incorrect" });


            return Ok(user);
        }
    }
}