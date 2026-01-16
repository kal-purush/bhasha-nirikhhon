using BookWebEcommerce.Data;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using BookWebEcommerce.Models;
using Microsoft.AspNetCore.Mvc.Rendering;
using X.PagedList;
using X.PagedList.Mvc.Core;
using System.Xml.Serialization;
using static System.Reflection.Metadata.BlobBuilder;

namespace BookWebEcommerce.Controllers
{
    public class BookController : Controller
    {
        AppDbContext ApDb;

        public BookController(AppDbContext apDb)
        {
            ApDb = apDb;
        }
        public IActionResult Index(int ? page, int ? pagesize)// so ban ghi tren 1 trag
        {
            if(page == null)
            {
                page = 1;
            }
            if(pagesize == null)
            {
                pagesize = 3;
            }
            ViewBag.pageSize = pagesize;
            var books = ApDb.Books.Include(b => b.Author).ToList();
            return View(books.ToPagedList((int)page,(int)pagesize));
        }
        //public IActionResult ShowCategory(int? Category)
        //{
        //    Category = Category ?? 0;
        //    var BookCategorys = BookCategory.
        //    return View();
        //}
        public async Task<IActionResult> Details(int? id)
        {
            if(id == null || ApDb.Books== null)
            {
                return NotFound();
            }
            var books = await ApDb.Books.Include(l =>l.Author ).Include(p=> p.Publisher).Include(t=>t.Translator).FirstOrDefaultAsync(m=>m.Id == id);
            if(books == null)
            {
                return NotFound();
            }
            return View(books);
        }
        //Get
        public async Task<IActionResult> Create()
        {
            
            ViewBag.AuthorId = new SelectList(ApDb.Authors, "Id", "FullName");
            ViewBag.PublisherId = new SelectList(ApDb.Publishers, "Id", "Name");
            ViewBag.TranslatorId = new SelectList(ApDb.Translators, "Id", "Name");
            return View();
        }
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("Name", "Description", "Price", "ImageURL", "Category", "AuthorId", "PublisherId", "TranslatorId")] Book books)
        {
            if(ModelState.IsValid)
            {
                ApDb.Add(books);
                await ApDb.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }
            ViewBag.AuthorId = new SelectList(ApDb.Authors, "Id", "FullName");
            ViewBag.PublisherId = new SelectList(ApDb.Publishers, "Id", "Name");
            ViewBag.PranslatorId = new SelectList(ApDb.Translators, "Id", "Name");
            return View();
        }

        //get
        public IActionResult Edit(int id)
        {
            if(id == null|| ApDb.Books == null)
            {
                return NotFound();
            }
            var books = ApDb.Books.Find(id);
            if(books == null)
            {
                return NotFound();
            }
            ViewBag.AuthorId = new SelectList(ApDb.Authors, "Id", "FullName", books.AuthorId);
            ViewBag.PublisherId = new SelectList(ApDb.Publishers, "Id", "Name", books.PublisherId);
            ViewBag.TranslatorId = new SelectList(ApDb.Translators, "Id", "Name",books.TranslatorId);
            return View(books);
        }
        [HttpPost]
        [ValidateAntiForgeryToken]
        public IActionResult Edit(int id, [Bind("Name", "Description",  "Price", "ImageURL", "Category", "AuthorId", "PublisherId", "TranslatorId")] Book books)
        { 
			books.Id = id;
			if (id != books.Id)
            {
                return NotFound();
            }
            if(ModelState.IsValid)
            {
                try
                {
                    ApDb.Update(books);
                    ApDb.SaveChanges();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!BookExits(books.Id))
                    {
                        return NotFound();
                    }
                    else
                    {
                        throw;
                    }
                }
				return RedirectToAction(nameof(Index));
			}
			ViewBag.AuthorId = new SelectList(ApDb.Authors, "Id", "FullName", books.AuthorId);
			ViewBag.PublisherId = new SelectList(ApDb.Publishers, "Id", "Name", books.PublisherId);
			ViewBag.TranslatorId = new SelectList(ApDb.Translators, "Id", "Name", books.TranslatorId);
			return View(books);

		}

        private bool BookExits(int id)
        {
            return (ApDb.Books?.Any(e => e.Id == id)).GetValueOrDefault();
        }
        //get
        public async Task<IActionResult> Delete(int id)
        {
            if(id == null || ApDb.Books == null)
            {
                return NotFound();
            }    
            var book = await ApDb.Books.Include(a => a.Author).Include(p => p.Publisher).Include(t => t.Translator).FirstOrDefaultAsync(m => m.Id == id);
            if(book==null)
            {
                return NotFound();
            }    
            return View(book);
        }
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public IActionResult DeleteConfirmed(int id)
        {
            if(ApDb.Books == null)
            {
                return Problem("Entity set 'Learners' is null");
            }
            var book = ApDb.Books.Find(id);
            if(book != null)
            {
                ApDb.Books.Remove(book);
            }
            ApDb.SaveChanges();
            return RedirectToAction(nameof(Index));
        }
    }
}