using Microsoft.AspNetCore.Mvc;
using BookWebEcommerce.Data;
using Microsoft.EntityFrameworkCore;
using BookWebEcommerce.Models;
using Microsoft.AspNetCore.Mvc.Rendering;
using X.PagedList;
namespace BookWebEcommerce.Controllers
{
	public class PublisherController : Controller
	{
		AppDbContext db;
		public PublisherController(AppDbContext db) { this.db = db; }
		[HttpGet]
		public IActionResult Index(string currentFilter, string searchString, int? page)
		{
			var lstPub = new List<Publisher>();

            if (searchString != null)
            {
                page = 1;
            }
            else
            {
				//chuoi tim kiem = moi gia tri nhap vao searchbox
                searchString = currentFilter;
            }
            ViewBag.CurrentFilter = searchString;
            if (!String.IsNullOrEmpty(searchString))
            {
				lstPub = db.Publishers.Where(s => s.Name.ToUpper().Contains(searchString.ToUpper())).ToList();
			}
			else
			{
				lstPub = db.Publishers.ToList();
			}

			//sap xep theo chu cai
			lstPub = lstPub.OrderBy(n => n.Name).ToList();

            int pageNumber = (page ?? 1);
            return View(lstPub.ToPagedList((int)pageNumber,3));
		}

		[HttpGet]
        public async Task<IActionResult> Create()
		{
			ViewBag.PublisherId = new SelectList(db.Publishers, "Id", "Name");

			return View();
		}

		[HttpPost]
		[ValidateAntiForgeryToken]
		public async Task<IActionResult> Create([Bind("Logo,Name,Description")]Publisher publisher)
		{
			////publisher.Id = (db.Publishers.Max(m => (int?)m.Id) ?? 0) + 1;
			if(ModelState.IsValid)
			{
				db.Publishers.Add(publisher);
                await db.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
			}
			ViewBag.PublisherId = new SelectList(db.Publishers, "Id", "Name");
			return View();
		}

		[HttpGet]
		public ActionResult Details(int id)
		{
			if(id == null || db.Publishers == null)
			{
				return NotFound();
			}
			var publisher = db.Publishers.Include(n => n.Books).Where(m => m.Id == id).FirstOrDefault();
			if (publisher == null)
			{
				return NotFound();
			}	
			return View(publisher);
		}

		
		private bool PublisherExists(int id)
				{
					return (db.Publishers?.Any(e => e.Id == id)).GetValueOrDefault();
				}
		[HttpGet]
		public ActionResult Edit(int id)
		{
			if (id == null || db.Publishers == null)
			{
				return NotFound();
			}
			var publisher = db.Publishers.Include(n => n.Books).Where(m => m.Id == id).FirstOrDefault();
			if (publisher == null)
			{
				return NotFound();
			}
			return View(publisher);
		}


        [HttpPost]
        [ValidateAntiForgeryToken]
        public IActionResult Edit(int id, [Bind("Logo,Name,Description")]Publisher publisher)
		{
			publisher.Id = id;
			if(id != publisher.Id)
			{
				return NotFound();
			}
			if (ModelState.IsValid)
			{
				try
				{
					db.Update(publisher);
					db.SaveChanges();
				}catch(DbUpdateConcurrencyException)
				{
					if(!PublisherExists(publisher.Id))
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
			return View(publisher);
		}


        [HttpGet]
        public ActionResult Delete(int id)
        {
            if (id == null || db.Publishers == null)
            {
                return NotFound();
            }
            var publisher = db.Publishers.Include(n => n.Books).Where(m => m.Id == id).FirstOrDefault();
            if (publisher == null)
            {
                return NotFound();
            }
            return View(publisher);
        }
        [HttpPost]
        [ValidateAntiForgeryToken]

		public ActionResult DeletePublisher(int id)
		{
			var item = db.Publishers.Find(id);
			if (item == null)
			{
				return NotFound();
			}
			var book = db.Books.Where(m=>m.PublisherId == id);
			db.Books.RemoveRange(book);
			db.Publishers.Remove(item);
			db.SaveChanges();
			return RedirectToAction(nameof(Index));
		}
    }
}