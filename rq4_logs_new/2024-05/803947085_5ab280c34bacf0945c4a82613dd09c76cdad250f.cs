using Event_Planning_System.DTO;
using Event_Planning_System.IServices;
using Microsoft.AspNetCore.Mvc;

namespace Event_Planning_System.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class RigsterController : Controller
    {
        
        readonly Iregestration _iregestration;
        public RigsterController(Iregestration iregestration) {
          this._iregestration = iregestration;
        }
        [HttpPost]
        public async Task<ActionResult> AddUser([FromBody] UserDto userDto, [FromQuery] string password)
        {
            await _iregestration.AddUserAsync(userDto, password);
            return Ok();
        }
    }
}