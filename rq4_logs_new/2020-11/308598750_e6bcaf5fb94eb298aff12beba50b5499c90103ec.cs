using api.layer.BusinessLayer;
using Microsoft.AspNetCore.Mvc;

namespace api.layer.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class ReportController : Controller
    {
        private readonly IGitActionsManager _gitActionsManager;

        public ReportController(IGitActionsManager gitActionsManager)
        {
            _gitActionsManager = gitActionsManager;
        }

        [HttpGet]
        public IActionResult Get()
        {
            return Ok(_gitActionsManager.FetchRaitingReport());
        }
    }
}