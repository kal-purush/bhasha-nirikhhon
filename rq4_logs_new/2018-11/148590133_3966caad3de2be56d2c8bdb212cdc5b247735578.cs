using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using Database;
using LibraryObjects;

namespace RESTServer.Controllers
{
    public class BookController : ApiController
    {
        BookDBHelper bookDBHelper = new BookDBHelper();

        // GET: api/Book
        public IEnumerable<Book> Get()
        {
            return bookDBHelper.GetAllBooks();
        }

        // GET: api/Book/5
        public Book Get(int id)
        {
            return bookDBHelper.GetBookById(id);
        }

        // POST: api/Book
        public HttpResponseMessage Post([FromBody]Book newBook)
        {
            int newBookId = bookDBHelper.AddNewBook(newBook);
            HttpResponseMessage response = Request.CreateResponse(HttpStatusCode.Created);
            response.Headers.Location = new Uri(Request.RequestUri, String.Format("book/{0}", newBookId));
            return response;
        }

        // PUT: api/Book/5
        public HttpResponseMessage Put(int id, [FromBody]Book newBook)
        {
            bool successfulEdit = bookDBHelper.EditBook(id, newBook);
            HttpResponseMessage response;
            if (successfulEdit)
            {
                response = Request.CreateResponse(HttpStatusCode.NoContent);
            }
            else
            {
                response = Request.CreateResponse(HttpStatusCode.NotFound);
            }
            return response;
        }

        // DELETE: api/Book/5
        public HttpResponseMessage Delete(int id)
        {
            bool successfullDelete = bookDBHelper.DeleteBook(id);
            HttpResponseMessage response;
            if (successfullDelete)
            {
                response = Request.CreateResponse(HttpStatusCode.NoContent);
            }
            else
            {
                response = Request.CreateResponse(HttpStatusCode.NotFound);
            }
            return response;
        }
    }
}