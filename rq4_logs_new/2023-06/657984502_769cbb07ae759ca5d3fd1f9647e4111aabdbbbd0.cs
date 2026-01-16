using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using ToDoList.Services;

namespace ToDoList.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class TasksController : Controller
    {
        private readonly ITasksService _tasksService;
        public TasksController(ITasksService tasksService)
        {
            _tasksService = tasksService;
        }

        [HttpGet]
        public ActionResult GetTasks()
        {          
            return Ok(_tasksService.GetTasks());
        }

        [HttpGet("{id}")]
        public ActionResult GetTask([FromRoute] Guid id)
        {
            try
            {
                 return Ok(_tasksService.GetTask(id));
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }
        [HttpPost]
        public ActionResult CreateTask([FromQuery] string name, string content)
        {
            _tasksService.CreateTask(name, content);
            return Ok();
        }
        [HttpPut]
        public ActionResult UpdateTask([FromQuery] ToDoTask updateTask)
        {
            try
            {
                _tasksService.UpdateTask(updateTask);
                return Ok();
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }
        [HttpDelete]
        public ActionResult DeleteTask(Guid id)
        {
            try
            {
                _tasksService.DeleteTask(id);
                return Ok();
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }
    }
}