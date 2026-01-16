using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Lab19_API.Data;
using Lab19_API.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace Lab19_API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class TodoController : ControllerBase
    {
        private readonly TodoDBContext _context;

        public TodoController(TodoController context)
        {
            _context = context;
        }

        [HttpGet]
        public IEnumerable<Todo> GetTodos()
        {
            return _context.TodoItems;
        }

        // GET: api/<controller>
        [HttpGet("{id]")]
        public async Task<IActionResult> GetTodo([FromRoute] int id)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            var todo = await _context.TodoItems.FindAsync(id);

            if (todo == null)
            {
                return NotFound();
            }

            return Ok(todo);
        }

        // POST api/<controller>
        [HttpPost]
        public async Task<IActionResult> Post([FromBody] Todo todo)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            //Add todo to Todos
            await _context.TodoItems.AddAsync(todo);
            await _context.SaveChangesAsync();

            //Send back 201 status "created"
            return CreatedAtRoute("GetByID", new { id = todo.ID }, todo);
        }

        // PUT api/<controller>/5
        [HttpPut("{id}")]
        public async Task<IActionResult> Put(int id, [FromBody] Todo todo)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }
            var result = _context.TodoItems.FirstOrDefault(x => x.ID == id);

            if (result != null)
            {
                result.Name = todo.Name;
                result.IsComplete = todo.IsComplete;
                await _context.SaveChangesAsync();
            }
            else
            {
                await Post(todo);
            }

            return NoContent();
        }

        // DELETE api/<controller>/5
        [HttpDelete("{id}")]
        public IActionResult Delete(int id)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }
            var result = _context.TodoItems.FirstOrDefault(x => x.ID == id);
            if (result != null)
            {
                _context.TodoItems.Remove(result);
                _context.SaveChanges();
            }
            return NoContent();
        }
    }
}