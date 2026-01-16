using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using ProductManagementSystem.StevieTV.Data;
using ProductManagementSystem.StevieTV.Models;

namespace ProductManagementSystem.StevieTV.Controllers
{
    public class VideoGameController : Controller
    {
        private readonly VideoGameContext _context;

        public VideoGameController(VideoGameContext context)
        {
            _context = context;
        }

        // GET: VideoGame
        // public async Task<IActionResult> Index()
        // {
        //     return View(await _context.VideoGames.ToListAsync());
        // }
        public async Task<IActionResult> Index(string sortOrder)
        {
            ViewBag.NameSortParm = String.IsNullOrEmpty(sortOrder) ? "name_desc" : "";
            ViewBag.DateSortParm = sortOrder == "date" ? "date_desc" : "date";
            ViewBag.ActiveSortParm = sortOrder == "active" ? "active_desc" : "active";
            ViewBag.PriceSortParm = sortOrder == "price" ? "price_desc" : "price";

            var videoGames = await _context.VideoGames.ToListAsync();

            switch (sortOrder)
            {
                case "name_desc":
                    videoGames = videoGames.OrderByDescending(v => v.Name).ToList();
                    break;
                case  "date":
                    videoGames = videoGames.OrderBy(v => v.DateAdded).ToList();
                    break;
                case "date_desc":
                    videoGames = videoGames.OrderByDescending(v => v.DateAdded).ToList();
                    break;
                case  "active":
                    videoGames = videoGames.OrderBy(v => v.IsActive).ToList();
                    break;
                case "active_desc":
                    videoGames = videoGames.OrderByDescending(v => v.IsActive).ToList();
                    break;
                case  "price":
                    videoGames = videoGames.OrderBy(v => v.Price).ToList();
                    break;
                case "price_desc":
                    videoGames = videoGames.OrderByDescending(v => v.Price).ToList();
                    break;
                default:
                    videoGames = videoGames.OrderBy(v => v.Name).ToList();
                    break;
            }

            return View(videoGames);
        }

        // GET: VideoGame/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var videoGame = await _context.VideoGames
                .FirstOrDefaultAsync(m => m.Id == id);
            if (videoGame == null)
            {
                return NotFound();
            }

            return View(videoGame);
        }

        // GET: VideoGame/Create
        public IActionResult Create()
        {
            return View();
        }

        // POST: VideoGame/Create
        // To protect from overposting attacks, enable the specific properties you want to bind to.
        // For more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("Id,Name,IsActive,DateAdded,Price")] VideoGame videoGame)
        {
            if (ModelState.IsValid)
            {
                _context.Add(videoGame);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }
            return View(videoGame);
        }

        // GET: VideoGame/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var videoGame = await _context.VideoGames.FindAsync(id);
            if (videoGame == null)
            {
                return NotFound();
            }
            return View(videoGame);
        }

        // POST: VideoGame/Edit/5
        // To protect from overposting attacks, enable the specific properties you want to bind to.
        // For more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("Id,Name,IsActive,DateAdded,Price")] VideoGame videoGame)
        {
            if (id != videoGame.Id)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(videoGame);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!VideoGameExists(videoGame.Id))
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
            return View(videoGame);
        }

        // GET: VideoGame/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var videoGame = await _context.VideoGames
                .FirstOrDefaultAsync(m => m.Id == id);
            if (videoGame == null)
            {
                return NotFound();
            }

            return View(videoGame);
        }

        // POST: VideoGame/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            var videoGame = await _context.VideoGames.FindAsync(id);
            if (videoGame != null)
            {
                _context.VideoGames.Remove(videoGame);
            }

            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }

        private bool VideoGameExists(int id)
        {
            return _context.VideoGames.Any(e => e.Id == id);
        }
    }
}