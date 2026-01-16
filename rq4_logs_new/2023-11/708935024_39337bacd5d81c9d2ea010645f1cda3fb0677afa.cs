using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.EntityFrameworkCore;
using FaulknerCountyMuseumGallery.Data;
using FaulknerCountyMuseumGallery.Models;
using FaulknerCountyMuseumGallery.Models.GalleryViewModels;  // Add VM

namespace FaulknerCountyMuseumGallery.Pages
{
    public class GalleryModel : PageModel
    {
        private readonly FaulknerCountyMuseumGallery.Data.GalleryContext _context;
        private readonly IConfiguration Configuration;

        public GalleryModel(FaulknerCountyMuseumGallery.Data.GalleryContext context,
                            IConfiguration configuration)
        {
            _context = context;
            Configuration = configuration;
        }

        public string TitleSort { get; set; }
        public string ArtistSort { get; set; }
        public string MediumSort { get; set; }
        public string CurrentFilter { get; set; }
        public string CurrentSort { get; set; }

        public ArtworkIndexData ArtworkData { get; set; }
        public int ArtworkID { get; set; }

        public async Task OnGetAsync(int? id, int? mediumID, string sortOrder,
            string currentFilter, string searchString, int? pageIndex)
        {
            TitleSort = String.IsNullOrEmpty(sortOrder) ? "title_desc" : "";
            ArtistSort = sortOrder == "Artist" ? "artist_desc" : "Artist";
            MediumSort = sortOrder == "Medium" ? "medium_desc" : "Medium";

            if (searchString != null)
            {
                pageIndex = 1;
            }
            else
            {
                searchString = currentFilter;
            }

            CurrentFilter = searchString;

            IQueryable<Artwork> artworksIQ = from a in _context.Artworks select a;
            if (!String.IsNullOrEmpty(searchString))
            {
                artworksIQ = artworksIQ.Where(a => a.Title.Contains(searchString)
                                        || a.Artist.Name.Contains(searchString)
                                        || a.Medium.Description.Contains(searchString));
            }

            var pageSize = Configuration.GetValue("PageSize", 4);

            ArtworkData = new ArtworkIndexData();
            ArtworkData.Artworks = await PaginatedList<Artwork>.CreateAsync(
                artworksIQ
                .AsNoTracking()
                .Include(i => i.Artist)
                .Include(i => i.Medium), pageIndex ?? 1, pageSize);
            
            if (id != null)
            {
                ArtworkID = id.Value;
                Artwork artwork = ArtworkData.Artworks
                    .Where(i => i.ArtworkID == id.Value).Single();
                ArtworkData.Artist = artwork.Artist;
                ArtworkData.Medium = artwork.Medium;
            }
        }
    }
}