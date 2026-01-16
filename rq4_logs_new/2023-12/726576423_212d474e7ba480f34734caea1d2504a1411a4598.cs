using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using CSCI213_Assignment5.Data;
using CSCI213_Assignment5.Models;

namespace CSCI213_Assignment5.Controllers
{
    public class BrowseSongs : Controller
    {
        private readonly CSCI213_Assignment5Context _context;

        public BrowseSongs(CSCI213_Assignment5Context context)
        {
            _context = context;
        }

        // GET: BrowseSongs
        public async Task<IActionResult> Index(string SearchGenre, string SearchArtist)
        {
            if (_context.Song == null)
            {
                return Problem("Entity set '_context.song'  is null.");
            }

            IQueryable<string> genreQuery = from s in _context.Song
                                            orderby s.Genre
                                            select s.Genre;

            IQueryable<string> artistQuery = from s in _context.Song
                                             orderby s.Artist
                                             select s.Artist;

            var songs = from s in _context.Song
                        select s;

            if (!string.IsNullOrEmpty(SearchGenre))
            {
                songs = songs.Where(x => x.Genre == SearchGenre);
            }

            if (!string.IsNullOrEmpty(SearchArtist))
            {
                songs = songs.Where(x => x.Artist == SearchArtist);
            }

            var songVM = new SongViewModel
            {
                Genres = new SelectList(await genreQuery.Distinct().ToListAsync()),
                Artists = new SelectList(await artistQuery.Distinct().ToListAsync()),
                Songs = await songs.ToListAsync()
            };

            return View(songVM);
        }

            // GET: BrowseSongs/Details/5
            public async Task<IActionResult> Details(int? id)
        {
            if (id == null || _context.Song == null)
            {
                return NotFound();
            }

            var song = await _context.Song
                .FirstOrDefaultAsync(m => m.SongID == id);
            if (song == null)
            {
                return NotFound();
            }

            return View(song);
        }

        // GET: BrowseSongs/Create
        public IActionResult Create()
        {
            return View();
        }

        // POST: BrowseSongs/Create
        // To protect from overposting attacks, enable the specific properties you want to bind to.
        // For more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("SongID,Title,Artist,Price,Inventory,Genre,Instrumental")] Song song)
        {
            if (ModelState.IsValid)
            {
                _context.Add(song);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }
            return View(song);
        }

        // GET: BrowseSongs/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null || _context.Song == null)
            {
                return NotFound();
            }

            var song = await _context.Song.FindAsync(id);
            if (song == null)
            {
                return NotFound();
            }
            return View(song);
        }

        // POST: BrowseSongs/Edit/5
        // To protect from overposting attacks, enable the specific properties you want to bind to.
        // For more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("SongID,Title,Artist,Price,Inventory,Genre,Instrumental")] Song song)
        {
            if (id != song.SongID)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(song);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!SongExists(song.SongID))
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
            return View(song);
        }

        // GET: BrowseSongs/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null || _context.Song == null)
            {
                return NotFound();
            }

            var song = await _context.Song
                .FirstOrDefaultAsync(m => m.SongID == id);
            if (song == null)
            {
                return NotFound();
            }

            return View(song);
        }

        // POST: BrowseSongs/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            if (_context.Song == null)
            {
                return Problem("Entity set 'CSCI213_Assignment5Context.Song'  is null.");
            }
            var song = await _context.Song.FindAsync(id);
            if (song != null)
            {
                _context.Song.Remove(song);
            }
            
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }

        private bool SongExists(int id)
        {
          return (_context.Song?.Any(e => e.SongID == id)).GetValueOrDefault();
        }
    }
}