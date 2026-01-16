using FaulknerCountyMuseumGallery.Models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.EntityFrameworkCore;

namespace FaulknerCountyMuseumGallery.Pages
{
    [Authorize]
    public class AdminModel : PageModel
    {
        private readonly FaulknerCountyMuseumGallery.Data.GalleryContext _context;

        public AdminModel(FaulknerCountyMuseumGallery.Data.GalleryContext context)
        {
            _context = context;
        }

        [BindProperty]
        public IList<Artwork> Artworks { get; set; } = default!;
        public IList<Collection> Collections { get;set; } = default!;
        public IList<Artist> Artists { get; set; } = default!;
        public IList<Medium> Mediums { get; set; } = default!;

        public async Task OnGetAsync()
        {
            if (_context.Collections != null)
            {
                Collections = await _context.Collections.AsNoTracking().ToListAsync();
            }
            if (_context.Artworks != null)
            {
                Artworks = await _context.Artworks.AsNoTracking().ToListAsync();
            }
            if (_context.Artists != null)
            {
                Artists = await _context.Artists.AsNoTracking().ToListAsync();
            }
            if (_context.Mediums != null)
            {
                Mediums = await _context.Mediums.AsNoTracking().ToListAsync();
            }
        }
    }
}