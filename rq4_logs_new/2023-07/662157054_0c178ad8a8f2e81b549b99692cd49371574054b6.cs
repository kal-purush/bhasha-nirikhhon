using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using rb.api.ViewModels;
using rb.bll;
using rb.dal.Models;

namespace rb.api.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class EducationSkillController : Controller
    {
        private readonly ILogger<EducationSkillController> _logger;
        private readonly EducationSkillService educationSkillService;

        public EducationSkillController(ILogger<EducationSkillController> logger)
        {
            _logger = logger;
            educationSkillService = new EducationSkillService();
        }

        [Authorize]
        [HttpPost("AddEducationSkill")]
        public ActionResult AddEducationSkill(AddEducationSkill addEducationSkill)
        {
            EducationSkill? educationSkill = educationSkillService.AddEducationSkill(addEducationSkill.EducationId, addEducationSkill.SkillId);

            if (educationSkill != null)
            {
                return Ok(educationSkill);
            }
            return BadRequest("Invalid education skill information");
        }

        [Authorize]
        [HttpGet("GetEducationSkills")]
        public ActionResult GetEducationSkills(int educationId)
        {
            List<Skill>? skills = educationSkillService.GetEducationSkills(educationId);

            if (skills != null)
            {
                return Ok(skills);
            }
            return BadRequest("Wrong educationId or no skills added");
        }

        [Authorize]
        [HttpDelete("RemoveEducationSkill")]
        public ActionResult RemoveEducationSkill(int educationSkillId)
        {
            bool flag = educationSkillService.RemoveEducationSkill(educationSkillId);

            if (flag)
            {
                return Ok("Education Skill removed");
            }
            return BadRequest("Invalid id");
        }
    }
}