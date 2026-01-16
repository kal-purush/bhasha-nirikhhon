using Microsoft.AspNetCore.Mvc;
using BookWebEcommerce.Data;
using BookWebEcommerce.Models;
using Microsoft.EntityFrameworkCore;
using X.PagedList;

namespace BookWebEcommerce.Controllers
{
    public class AuthorController : Controller
    {
        AppDbContext db;
        int pageSize = 3;
        
        public AuthorController(AppDbContext db)
        {
            this.db = db;
        }
        public IActionResult Index(int? page)
        {
            var author = (from l in db.Authors select l).OrderBy(X => X.Id);
            int pageNum = (page ?? 1);
            var paging = author.ToPagedList(pageNum, pageSize);
            if (Request.Headers["X-Requested-With"] == "XMLHttpRequest")
            {
                return PartialView("Paginate", paging);
            }
            return View(paging);
        }
        public IActionResult Create()
        {
            return View();
        }
        [HttpPost]
        [ValidateAntiForgeryToken]
        public IActionResult Create(Author model)
        {
            if (ModelState.IsValid)
            {
                db.Authors.Add(model);
                db.SaveChanges();
                return RedirectToAction("Index");
            }
            return View(model);
        }

        public IActionResult Edit(int? id)
        {
            if (db.Authors == null || id == null)
            {
                return NotFound();
            }
            var model = db.Authors.Find(id);
            if (model == null)
            {
                return NotFound();
            }
            return View(model);
        }
        [HttpPost]
        [ValidateAntiForgeryToken]
        public IActionResult Edit(int id, [Bind("Id,ProfilePictureURL,FullName,Bio")] Author model)
        {
            if (id != model.Id)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    db.Update(model);
                    db.SaveChanges();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!AuthorExits(model.Id))
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
            return View(model);
        }
        private bool AuthorExits(int? id)
        {
            return (db.Authors?.Any(e => e.Id == id)).GetValueOrDefault();
        }
        public IActionResult Delete(int? id)
        {
            if (db.Authors == null || id == null)
            {
                return NotFound();
            }
            var model = db.Authors.Include(e => e.Books).FirstOrDefault(m => m.Id == id);
            if (model == null)
            {
                return NotFound();
            }
            if (model.Books.Count() > 0)
            {
                return Content("This author has some books, can't delete!");
            }
            return View(model);
        }
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public IActionResult Deletee(int id)
        {
            if (db.Authors == null)
            {
                return Problem("Entity set 'Authors' is null");
            }
            var model = db.Authors.Find(id);
            if (model != null)
            {
                db.Authors.Remove(model);
            }
            db.SaveChanges();
            return RedirectToAction(nameof(Index));
        }
        public IActionResult Details(int? id)
        {
            if (db.Authors == null || id == null)
            {
                return NotFound();
            }
            //var model = db.Authors.Find(id);
            var model = db.Authors.Include(e => e.Books).FirstOrDefault(m => m.Id == id);
            if (model == null)
            {
                return NotFound();
            }

            if (model.Books.Count() > 0)
            {
                ViewBag.book = db.Books.Where(m => m.AuthorId == id).Select(n => n.Name).ToList();
            }
            else
            {
                ViewBag.book = null;
            }
            return View(model);
        }
        
    }
}