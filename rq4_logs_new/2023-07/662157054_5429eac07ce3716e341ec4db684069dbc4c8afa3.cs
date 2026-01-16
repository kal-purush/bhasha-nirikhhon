using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using rb.bll;

namespace rb.api.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class CertificateController : Controller
    {
        private readonly ILogger<UserController> _logger;
        private readonly CertificateService certificateService;

        public CertificateController(ILogger<UserController> logger)
        {
            _logger = logger;
            certificateService = new CertificateService();
        }

        [Authorize]
        [HttpGet]
        public ActionResult Test()
        {
            return Ok("test");
        }
    }
}