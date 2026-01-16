using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using teampomodoroapi.DataAccess;
using teampomodoroapi.Models;

namespace teampomodoroapi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class TaskController : ControllerBase
    {
        private readonly TasksStorage _storage;

        public TaskController(IConfiguration config)
        {
            _storage = new TasksStorage(config);
        }

        // post new task
        [HttpPost]
        public IActionResult Post(Tasks newTask)
        {
            var result = _storage.CreateTask(newTask);

            if (result != null)
            {
                return Ok(result);
            }
            return BadRequest();
        }

        // update task
        [HttpPut]
        public IActionResult Put(Tasks newTask)
        {
            var result = _storage.UpdateTask(newTask);

            if (result != null)
            {
                return Ok(result);
            }
            return NotFound();
        }

    }
}