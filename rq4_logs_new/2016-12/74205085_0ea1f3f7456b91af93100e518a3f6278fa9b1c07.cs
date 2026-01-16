using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using BookStore.Models;
using BookStore.Models.Entities;

namespace BookStore.Controllers
{
    public class AdminController : Controller
    {
        private BooksStoreContext _context = new BooksStoreContext();
        // GET: Admin
        [HttpGet]
        public ActionResult AddBook()
        {
            return View();
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult AddBook(Book book)
        {
            //if (ModelState.IsValid)
            //{
            _context.Books.Add(book);
            _context.SaveChanges();
                return RedirectToAction("Index", "Books");
            //}

        }
    }
}