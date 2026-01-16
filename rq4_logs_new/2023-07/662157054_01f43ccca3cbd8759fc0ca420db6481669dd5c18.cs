using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using rb.api.ViewModels;
using rb.bll;
using rb.dal.Models;

namespace rb.api.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class EducationController : Controller
    {
        private readonly ILogger<EducationController> _logger;
        private readonly EducationService educationService;

        public EducationController(ILogger<EducationController> logger)
        {
            _logger = logger;
            educationService = new EducationService();
        }

        [Authorize]
        [HttpPost("AddEducation")]
        public ActionResult AddEducation(AddEducation addEducation)
        {
            Education? education = educationService.AddEducation(addEducation.Place , addEducation.FromDate, addEducation.ToDate, addEducation.UserId);

            if (education != null)
            {
                return Ok(education);
            }
            return BadRequest("Invalid education information");
        }

        [Authorize]
        [HttpDelete("RemoveEducation")]
        public ActionResult RemoveCertificate(int certificateId)
        {
            bool flag = educationService.RemoveEducation(certificateId);

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
            List<Education>? educations = educationService.GetAllByUserId(userId);

            if (educations != null)
            {
                return Ok(educations);
            }
            return BadRequest("Wrong userId or no educations added");
        }
    }
}