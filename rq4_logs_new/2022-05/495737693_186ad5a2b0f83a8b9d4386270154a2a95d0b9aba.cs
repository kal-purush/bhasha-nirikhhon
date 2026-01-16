using LibraryItems.Models;
using LibraryItems.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace LibraryItems.Controllers
{
    [Route("[controller]")]
    [ApiController]

    public class LibraryController : ControllerBase
    {
        private readonly ICrudService<Library, int> _libraryService;
        public LibraryController(ICrudService<Library, int> libraryService)
        {
            _libraryService = libraryService;
        }

        // GET all action
        [HttpGet]
        public ActionResult<List<Library>> GetAll() => _libraryService.GetAll().ToList();

        // GET by Id action
        [HttpGet("{id}")]
        public ActionResult<Library> Get(int id)
        {
            var library = _libraryService.Get(id);
            if (library is null) return NotFound();
            else return library;
        }

        // POST action
        [HttpPost]
        public IActionResult Create(Library library)
        {
            // Runs validation against model using data validation attributes
            if (ModelState.IsValid)
            {
                _libraryService.Add(library);
                return CreatedAtAction(nameof(Create), new { id = library.Id }, library);
            }
            return BadRequest();
        }

        // PUT action
        [HttpPut("{id}")]
        public IActionResult Update(int id, Library library)
        {
            var existingLibrary = _libraryService.Get(id);
            if (existingLibrary is null || existingLibrary.Id != id)
            {
                return BadRequest();
            }
            if (ModelState.IsValid)
            {
                _libraryService.Update(existingLibrary, library);
                return NoContent();
            }
            return BadRequest();
        }

        // DELETE action
        [HttpDelete("{id}")]
        public IActionResult Delete(int id)
        {
            var library = _libraryService.Get(id);
            if (library is null) return NotFound();
            _libraryService.Delete(id);
            return NoContent();
        }
    }
}