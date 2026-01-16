
using ControllerApp.Domains.Users;
using ControllerApp.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace ControllerApp.Controllers
{
    [Route("api/users")]
    [ApiController]
    public class UsersController : ControllerBase
    {
        private readonly IUserInterface _userInterface;

        public UsersController(IUserInterface userInterface)
        {
            _userInterface = userInterface;
        }

        [HttpGet]
        public IActionResult GetUsers()
        {
            var users = _userInterface.GetUsers();
            return Ok(users);
        }

        [HttpGet("{id}")]
        public IActionResult GetUserById(int id)
        {
            var user = _userInterface.GetUser(id);
            if(user == null)
            {
                return NotFound($"User with Id {id} was not found contact Stanley");
            }
            return Ok(user);
        }

        [HttpPost]
        public IActionResult AddUser([FromBody]User user)
        {
            if(user == null)
            {
                return BadRequest("User is null are you crazy");
            }
            var u = _userInterface.AddUser(user);
            return Ok(u);
        }

        [HttpPut("{id}")]
        public IActionResult UpdateUser(int id, [FromBody]User user)
        {
            if (user == null)
            {
                return BadRequest("User is null");
            }
            var u = _userInterface.GetUser(id);
            if(u == null)
            {
                return NotFound("User was not found contact Stanley");
            }
            _userInterface.UpdateUser(u,user);
            return NoContent();
        }

        [HttpDelete("{id}")]
        public IActionResult DeleteUser(int id)
        {           
            var u = _userInterface.GetUser(id);
            if (u == null)
            {
                return NotFound("User was not found contact Stanley");
            }
            _userInterface.DeleteUser(u);
            return NoContent();
        }
    }
}