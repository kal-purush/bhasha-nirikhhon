using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNet.Mvc;
using BookStore;

// For more information on enabling Web API for empty projects, visit http://go.microsoft.com/fwlink/?LinkID=397860

namespace Demo.MVC6.Controllers.Api
{
    [Route("api/[controller]")]
    public class BooksController : Controller
    {

        public BooksController(IBookReader reader)
        {
            Reader = reader;
        }

        IBookReader Reader { get; }

        [HttpGet]
        public IActionResult Get()
        {
            return Json(new { Foo = "Blah" });
        }

        [HttpGet("{id}")]
        public string Get(int id)
        {
            var book = Reader.Read(id);
            return "value";
        }

        [HttpPost]
        public void Post([FromBody]string value)
        {
        }

        [HttpPut("{id}")]
        public void Put(int id, [FromBody]string value)
        {
        }

    }
}