using System;
using LibraryApi.Services;
using Microsoft.AspNetCore.Mvc;

namespace LibraryApi.Controllers
{
    [Route("api/library")]
    public class LibraryController : Controller
    {
        private readonly ILibraryService _libraryService;

        public LibraryController(ILibraryService libraryService)
        {
            _libraryService = libraryService;
            _libraryService.BooksLoadJson();
            _libraryService.PersonsLoadJson();
        }

        [HttpGet("")]
        public IActionResult GetBallBooks()
        {
            var books = _libraryService.GetAllBooks();

            return Ok(books);
        }

        [HttpGet("persons")]
        public IActionResult GetAllPersons()
        {
            var persons = _libraryService.GetAllPersons();
            return Ok(persons);
        }
    }

}