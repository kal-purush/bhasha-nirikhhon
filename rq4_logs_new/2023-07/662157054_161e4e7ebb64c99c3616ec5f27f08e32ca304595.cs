using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using rb.api.ViewModels;
using rb.api.ViewModels.Certificate;
using rb.bll;
using rb.dal.Models;

namespace rb.api.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class TemplateController : Controller
    {
        private readonly ILogger<TemplateController> _logger;
        private readonly TemplateService templateService;

        public TemplateController(ILogger<TemplateController> logger)
        {
            _logger = logger;
            templateService = new TemplateService();
        }

        [Authorize]
        [HttpPost("AddTemplate")]
        public ActionResult AddTemplate(AddTemplate addTemplate)
        {
            Template? template = templateService.AddTemplate(addTemplate.Id, addTemplate.UserId);

            if (template != null)
            {
                return Ok(template);
            }
            return BadRequest("Invalid template information");
        }

        [Authorize]
        [HttpDelete("RemoveTemplate")]
        public ActionResult RemoveCertificate(int certificateId)
        {
            bool flag = templateService.RemoveTemplate(certificateId);

            if (flag)
            {
                return Ok("Education removed");
            }
            return BadRequest("Invalid id");
        }

        [Authorize]
        [HttpGet("GetAllByUserId")]
        public ActionResult GetAllByUserId(int userId)
        {
            List<Template>? templates = templateService.GetAllByUserId(userId);

            if (templates != null)
            {
                return Ok(templates);
            }
            return BadRequest("Wrong userId or no templates added");
        }

        [Authorize]
        [HttpPatch("EditTemplate")]
        public ActionResult EditTemplate(EditTemplate editTemplate)
        {
            Template? template = templateService.EditTemplate(editTemplate.Id);

            if (template != null)
            {
                return Ok(template);
            }
            return BadRequest("Wrong input");
        }
    }
}