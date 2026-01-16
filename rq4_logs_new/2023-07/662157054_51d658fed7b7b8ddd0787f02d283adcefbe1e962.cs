using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.IdentityModel.Tokens;
using rb.bll;
using rb.dal.Models;

namespace rb.api.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class LanguageController : Controller
    {
        private readonly ILogger<LanguageController> _logger;
        private readonly LanguageService languageService;

        public LanguageController(ILogger<LanguageController> logger)
        {
            _logger = logger;
            languageService = new LanguageService();
        }

        [Authorize]
        [HttpGet("GetAll")]
        public ActionResult GetAll()
        {
            List<Language>? languages = languageService.GetAll();

            if (!languages.IsNullOrEmpty())
            {
                return Ok(languages);
            }
            return BadRequest("No languages added");
        }
    }
}