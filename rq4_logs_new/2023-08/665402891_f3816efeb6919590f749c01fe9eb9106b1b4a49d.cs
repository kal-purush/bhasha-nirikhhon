using AutoMapper;
using Calendar.Api.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace Calendar.Api.Controllers
{
    [ApiController]
    [Route("v1/api/[controller]")]
    public class KeycloakController : ControllerBase
    {
        private readonly IMapper mapper;
        private readonly ILogger<LectureController> logger;

        public KeycloakController(IMapper mapper, ILogger<LectureController> logger)
        {
            this.mapper = mapper;
            this.logger = logger;
        }

        [HttpGet]
        [Authorize(AuthPolicies.EDITOR_VIEWER)]
        public async Task<ActionResult> GetAssignedCalendars()
        {
            return Ok();
        }

        [HttpGet]
        [Authorize(AuthPolicies.EDITOR)]
        public async Task<ActionResult> GetInstructors()
        {
            return Ok();
        }

        [HttpGet]
        [Authorize(AuthPolicies.EDITOR)]
        public async Task<ActionResult> GetSemester()
        {
            return Ok();
        }
    }
}