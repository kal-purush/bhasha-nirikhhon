using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CompetitionApp.Models;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Drawing;
using System.Drawing.Imaging;

namespace CompetitionApp.Controllers
{
    public class NewsController : Controller
    {
        private readonly ApplicationContext _context;
        private readonly UserManager<User> _userManager;

        public NewsController(UserManager<User> userManager, ApplicationContext context)
        {
            _context = context;
            _userManager = userManager;
        }

        private byte[] ImageToBytes(Image img, ImageFormat format)
        {
            MemoryStream mstream = new MemoryStream();
            img.Save(mstream, format);
            mstream.Flush();
            return mstream.ToArray();
        }


        // GET: News
        public async Task<IActionResult> Index()
        {
            return View(await _context.News.ToListAsync());
        }

        // GET: News/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var @event = await _context.News
                .FirstOrDefaultAsync(m => m.Id == id);
            if (@event == null)
            {
                return NotFound();
            }

            return View(@event);
        }

        // GET: News/Create
        public IActionResult Create()
        {
            return View();
        }
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("Id,Title,PublicationDate,Content,Publicator,Image")] News @news)
        {
            if (ModelState.IsValid)
            {
                //var fileImage = HttpContext.Request.Form.Files["image"];
                

                news.PublicationDate = DateTime.Now;
                var userId = _userManager.GetUserId(HttpContext.User);
                news.PublicatorId = userId;
                //news.Image = ImageToBytes(Image.FromFile(fileImage.FileName), ImageFormat.Jpeg);
                _context.Add(@news);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }
            return View(@news);
        }

        // GET: News/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var @news = await _context.News.FindAsync(id);
            if (@news == null)
            {
                return NotFound();
            }
            return View(@news);
        }
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("Id,Title,Content,Image")] News @news)
        {
            if (id != @news.Id)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(@news);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!NewsExists(@news.Id))
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
            return View(@news);
        }

        // GET: News/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var @news = await _context.News
                .FirstOrDefaultAsync(m => m.Id == id);
            if (@news == null)
            {
                return NotFound();
            }

            return View(@news);
        }
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            var @news = await _context.News.FindAsync(id);
            _context.News.Remove(@news);
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }

        private bool NewsExists(int id)
        {
            return _context.News.Any(e => e.Id == id);
        }
    }
}