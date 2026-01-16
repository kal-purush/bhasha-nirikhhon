using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CoreWebApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class BookStoreController : ControllerBase
    {
        private readonly static List<Book> _books;
        private readonly ILogger<BookStoreController> _logger;
        static BookStoreController()
        {
            _books = new List<Book>();
            _books.Add(new Book
            {
                Id = Guid.NewGuid(),
                Author = "Bulgakov",
                Title = "Master and Margarita",
                Genre = "Novel",
                Price = 5

            }) ;
        }

        public BookStoreController(ILogger<BookStoreController> logger)
        {
            _logger = logger;
        }

        [HttpPost("add")]
        public IActionResult Add(Book book)
        {
            if (book != null)
            {
                book.Id = Guid.NewGuid();
                _books.Add(book);

                return Created(book.Id.ToString(), book);
            }

            return BadRequest();
        }

        [HttpGet("all")]
        public IActionResult GetAll()
        {
            return Ok(_books);
        }

        [Route("[action]/{id}")]
        [HttpGet]
        public IActionResult GetById(Guid id)
        {
            var book = _books.FirstOrDefault(x => x.Id == id);

            return book != null ? Ok(book) : NotFound();
        }

        [Route("[action]/{author}")]
        [HttpGet]
        public IActionResult GetByAuthor(string author)
        {
            var bookList = _books.TakeWhile(car => car.Author == author);

            return bookList != null ? Ok(bookList) : NotFound();
        }

        [HttpPut]
        public IActionResult Update(Book book)
        {
            var dbBook = _books.FirstOrDefault(x => x.Id == book.Id);

            if (dbBook != null)
            {
                var index = _books.IndexOf(dbBook);
                _books[index] = book;

                return StatusCode(200);
            }

            return StatusCode(404);
        }

        [HttpDelete("{id}")]
        public IActionResult RemoveById(Guid id)
        {
            var dbBook = _books.FirstOrDefault(x => x.Id == id);

            if(dbBook != null)
            {
                _books.Remove(dbBook);

                return StatusCode(200);
            }

            return StatusCode(404);
        }
    }
}