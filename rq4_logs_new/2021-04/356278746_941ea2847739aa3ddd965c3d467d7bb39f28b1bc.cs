using Microsoft.AspNetCore.Mvc;

namespace Server.Controllers
{
    [Route("api/current")]
    [ApiController]
    public class CurrentController : ControllerBase
    {
        //get current customers
        // GET api/commands/1
        [HttpGet]
        public ActionResult<int> GetCurrent()
        {
            var commandItem = Server.Program.current;
            return Ok(commandItem);
        }
        //change max value
        // PUT api/commands/0
        [HttpPut("{id}")]
        public ActionResult Putcurrent(int id)
        {
            Server.Program.current = id;
            return NoContent();
        }
    }
}