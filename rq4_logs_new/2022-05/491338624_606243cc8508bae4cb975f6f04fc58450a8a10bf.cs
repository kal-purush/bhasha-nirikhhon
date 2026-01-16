using Microsoft.AspNetCore.Mvc;

namespace DevTrackR.API.Controllers
{
  [ApiController]
  [Route("api/packages")]
  public class PackagesController : ControllerBase
  {
    public IActionResult GetAll()
    {
      return Ok();
    }
  }
}