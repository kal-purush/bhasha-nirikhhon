using Microsoft.AspNetCore.Mvc;
using rb.api.ViewModels;
using rb.bll;
using rb.dal.Models;

namespace rb.api.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class UserController : ControllerBase
    {
        private readonly ILogger<UserController> _logger;
        private readonly UserService userService;

        public UserController(ILogger<UserController> logger)
        {
            _logger = logger;
            userService = new UserService();
        }

        [HttpPost("RegisterUser")]
        public bool Register([FromBody]RegisterUser registerUser)
        {
            return userService.RegisterUser(registerUser.Username, registerUser.Password, registerUser.Email);
        }

        [HttpPost("LoginUser")]
        public User Login([FromBody]LoginUser loginUser)
        {
            return userService.VerifyUser(loginUser.Username, loginUser.Password);
        }
    }
}