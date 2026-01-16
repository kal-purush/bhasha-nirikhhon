using Microsoft.AspNetCore.Mvc;

namespace Api.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class TestController : ControllerBase
    {
        [HttpGet("HelloWorld")]
        public ActionResult<string> HelloWorld()
        {
            return Ok("Hello World");
        }
    }
}