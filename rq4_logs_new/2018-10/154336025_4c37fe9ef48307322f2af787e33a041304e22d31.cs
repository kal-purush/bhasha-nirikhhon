using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using solution_MVC_Music.Data;
using solution_MVC_Music.Models;

namespace solution_MVC_Music.Controllers
{
    public class AlbumsController : Controller
    {
        private readonly MusicContext _context;

        public AlbumsController(MusicContext context)
        {
            _context = context;
        }

        // GET: Albums
        public async Task<IActionResult> Index()
        {
            var musicContext = _context.Albums.Include(a => a.Genre);
            return View(await musicContext.ToListAsync());
        }

        // GET: Albums/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var album = await _context.Albums
                .Include(a => a.Genre)
                .AsNoTracking()
                .FirstOrDefaultAsync(m => m.ID == id);
            if (album == null)
            {
                return NotFound();
            }

            return View(album);
        }

        // GET: Albums/Create
        public IActionResult Create()
        {
            PopulateDropDownLists();
            return View();
        }

        // POST: Albums/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("ID,Name,YearProduced,Price,GenreID")] Album album)
        {
            try
            {
                if (ModelState.IsValid)
                {
                    _context.Add(album);
                    await _context.SaveChangesAsync();
                    return RedirectToAction(nameof(Index));
                }
            }
            catch (DbUpdateException dex)
            {
                if (dex.InnerException.Message.Contains("IX_Albums_Name_YearProduced"))
                {
                    ModelState.AddModelError("", "Unable to save changes. Remember, you cannot have more then one album with the same name produced in the same year.");
                }
                else
                {
                    ModelState.AddModelError("", "Unable to save changes. Try again, and if the problem persists see your system administrator.");
                }
            }

            PopulateDropDownLists(album);
            return View(album);
        }

        // GET: Albums/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var album = await _context.Albums
                .Include(a => a.Genre)
                .Include(a => a.Songs)
                .AsNoTracking()
                .FirstOrDefaultAsync(a => a.ID == id);

            if (album == null)
            {
                return NotFound();
            }
            PopulateDropDownLists(album);
            return View(album);
        }

        // POST: Albums/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id)//, [Bind("ID,Name,YearProduced,Price,GenreID")] Album album)
        {
            var albumToUpdate = await _context.Albums
                .Include(a => a.Genre)
                .Include(a => a.Songs)
                .FirstOrDefaultAsync(a => a.ID == id);
            //Check that you got it or exit with a not found error
            if (albumToUpdate == null)
            {
                return NotFound();
            }

            //Try updating it with the values posted
            if (await TryUpdateModelAsync<Album>(albumToUpdate, "",
                a => a.Name, a => a.YearProduced, a=>a.Price, a=>a.GenreID))
            {
                try
                {
                    await _context.SaveChangesAsync();
                    return RedirectToAction(nameof(Index));
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!AlbumExists(albumToUpdate.ID))
                    {
                        return NotFound();
                    }
                    else
                    {
                        throw;
                    }
                }
                catch (DbUpdateException dex)
                {
                    if (dex.InnerException.Message.Contains("IX_Albums_Name_YearProduced"))
                    {
                        ModelState.AddModelError("", "Unable to save changes. Remember, you cannot have more then one album with the same name produced in the same year.");
                    }
                    else
                    {
                        ModelState.AddModelError("", "Unable to save changes. Try again, and if the problem persists see your system administrator.");
                    }
                }
            }
            //Validaiton Error so give the user another chance.
            PopulateDropDownLists(albumToUpdate);
            return View(albumToUpdate);
        }

        // GET: Albums/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var album = await _context.Albums
                .Include(a => a.Genre)
                .AsNoTracking()
                .FirstOrDefaultAsync(m => m.ID == id);
            if (album == null)
            {
                return NotFound();
            }

            return View(album);
        }

        // POST: Albums/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            var album = await _context.Albums
                .Include(a => a.Genre)
                .FirstOrDefaultAsync(m => m.ID == id);
            try
            {
                _context.Albums.Remove(album);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }
            catch (DbUpdateException dex)
            {
                if (dex.InnerException.Message.Contains("FK_Songs_Albums_AlbumID"))
                {
                    ModelState.AddModelError("", "Unable to save changes. You cannot delete a Album with songs on it.");
                }
                else
                {
                    ModelState.AddModelError("", "Unable to save changes. Try again, and if the problem persists see your system administrator.");
                }
            }
            return View(album);
        }

        //This is a twist on the PopulateDropDownLists approach
        //  Create methods that return each SelectList separately
        //  and one method to put them all into ViewData.
        //This approach allows for AJAX requests to refresh
        //DDL Data at a later date.
        private SelectList GenreSelectList(int? id)
        {
            var dQuery = from g in _context.Genres
                         orderby g.Name
                         select g;
            return new SelectList(dQuery, "ID", "Name", id);
        }
        private void PopulateDropDownLists(Album album = null)
        {
            ViewData["GenreID"] = GenreSelectList(album?.GenreID);
        }

        private bool AlbumExists(int id)
        {
            return _context.Albums.Any(e => e.ID == id);
        }
    }
}