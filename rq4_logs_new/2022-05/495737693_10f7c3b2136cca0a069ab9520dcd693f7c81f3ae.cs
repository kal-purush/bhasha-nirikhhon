using LibraryItems.Models;
using LibraryItems.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace LibraryItems.Controllers
{
    [Route("[controller]")]
    [ApiController]
    //need a controller to integrate with the service
    public class LibraryItemController : ControllerBase
    {
        private readonly ICrudService<LibraryItem, int> _libraryItemService;
        public LibraryItemController(ICrudService<LibraryItem, int> libraryItemService)
        {
            _libraryItemService = libraryItemService;
        }

        // GET all action
        [HttpGet]
        public ActionResult<List<LibraryItem>> GetAll() => _libraryItemService.GetAll().ToList();

        // GET by Id action
        [HttpGet("{id}")]
        public ActionResult<LibraryItem> Get(int id)
        {
            var todo = _libraryItemService.Get(id);
            if (todo is null) return NotFound();
            else return todo;
        }

        // POST action
        [HttpPost]
        public IActionResult Create(LibraryItem libItem)
        {
            // Runs validation against model using data validation attributes
            if (ModelState.IsValid)
            {
                _libraryItemService.Add(libItem);
                return CreatedAtAction(nameof(Create), new { id = libItem.Id }, libItem);
            }
            return BadRequest();
        }

        // PUT action
        [HttpPut("{id}")]
        public IActionResult Update(int id, LibraryItem libItem)
        {
            var existingLibItem = _libraryItemService.Get(id);
            if (existingLibItem is null || existingLibItem.Id != id)
            {
                return BadRequest();
            }
            if (ModelState.IsValid)
            {
                _libraryItemService.Update(existingLibItem, libItem);
                return NoContent();
            }
            return BadRequest();
        }

        // DELETE action
        [HttpDelete("{id}")]
        public IActionResult Delete(int id)
        {
            var todo = _libraryItemService.Get(id);
            if (todo is null) return NotFound();
            _libraryItemService.Delete(id);
            return NoContent();
        }
    }
}