using EightCare.API.Constants;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace EightCare.API.Controllers
{
    [ApiController]
    [Route(Routes.KeeperRoute)]
    public class KeepersController : ControllerBase
    {
        [HttpPost]
        [ProducesResponseType(StatusCodes.Status201Created)]
        public IActionResult CreateKeeper()
        {
            const int CreatedId = 1;

            return Created(Routes.KeeperRoute + $"/{CreatedId}", new { id = CreatedId });
        }
    }
}