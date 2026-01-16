using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using teampomodoroapi.DataAccess;
using teampomodoroapi.Models;

namespace teampomodoroapi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProjectsController : ControllerBase
    {
        private readonly ProjectsStorage _storage;

        public ProjectsController(IConfiguration config)
        {
            _storage = new ProjectsStorage(config);
        }

        // post new project
        [HttpPost]
        public IActionResult Post(ProjectWithUsers newProject)
        {
            var result = _storage.CreateProject(newProject);

            if (result != null)
            {
                return Ok(result);
            }
            return BadRequest();
        }

        [HttpPut]
        public IActionResult Put(Projects newProject)
        {
            var result = _storage.UpdateProject(newProject);

            if (result != null)
            {
                return Ok(result);
            }
            return NotFound();
        }
    }
}