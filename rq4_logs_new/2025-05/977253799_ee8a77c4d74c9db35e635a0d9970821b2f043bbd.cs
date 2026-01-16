using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using Movie.Data.Context;
using Movie.Domain.Models.TVShows;

namespace Movie_Web.Areas.Admin.Controllers
{
    [Area("Admin")]
    public class EpisodesController : Controller
    {
        private readonly MyMovieContext _context;

        public EpisodesController(MyMovieContext context)
        {
            _context = context;
        }

        // GET: Admin/Episodes
        public async Task<IActionResult> Index()
        {
            var myMovieContext = _context.Episodes.Include(e => e.Season);
            return View(await myMovieContext.ToListAsync());
        }

        // GET: Admin/Episodes/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var episodes = await _context.Episodes
                .Include(e => e.Season)
                .FirstOrDefaultAsync(m => m.Id == id);
            if (episodes == null)
            {
                return NotFound();
            }

            return View(episodes);
        }

        // GET: Admin/Episodes/Create
        public IActionResult Create()
        {
            ViewData["SeasonId"] = new SelectList(_context.Seasons, "Id", "SeasonDescription");
            return View();
        }

        // POST: Admin/Episodes/Create
        // To protect from overposting attacks, enable the specific properties you want to bind to.
        // For more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("ImageNameForEpisode,EpisodeName,EpisodeDescription,EpisodeNumber,EpisodeContentRating,ReleaseDate,EpisodeDuration,SeasonId,Id,CreateDate,ModifiedDate,IsDeleted")] Episodes episodes)
        {
            if (ModelState.IsValid)
            {
                _context.Add(episodes);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }
            ViewData["SeasonId"] = new SelectList(_context.Seasons, "Id", "SeasonDescription", episodes.SeasonId);
            return View(episodes);
        }

        // GET: Admin/Episodes/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var episodes = await _context.Episodes.FindAsync(id);
            if (episodes == null)
            {
                return NotFound();
            }
            ViewData["SeasonId"] = new SelectList(_context.Seasons, "Id", "SeasonDescription", episodes.SeasonId);
            return View(episodes);
        }

        // POST: Admin/Episodes/Edit/5
        // To protect from overposting attacks, enable the specific properties you want to bind to.
        // For more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("ImageNameForEpisode,EpisodeName,EpisodeDescription,EpisodeNumber,EpisodeContentRating,ReleaseDate,EpisodeDuration,SeasonId,Id,CreateDate,ModifiedDate,IsDeleted")] Episodes episodes)
        {
            if (id != episodes.Id)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(episodes);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!EpisodesExists(episodes.Id))
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
            ViewData["SeasonId"] = new SelectList(_context.Seasons, "Id", "SeasonDescription", episodes.SeasonId);
            return View(episodes);
        }

        // GET: Admin/Episodes/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var episodes = await _context.Episodes
                .Include(e => e.Season)
                .FirstOrDefaultAsync(m => m.Id == id);
            if (episodes == null)
            {
                return NotFound();
            }

            return View(episodes);
        }

        // POST: Admin/Episodes/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            var episodes = await _context.Episodes.FindAsync(id);
            if (episodes != null)
            {
                _context.Episodes.Remove(episodes);
            }

            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }

        private bool EpisodesExists(int id)
        {
            return _context.Episodes.Any(e => e.Id == id);
        }
    }
}