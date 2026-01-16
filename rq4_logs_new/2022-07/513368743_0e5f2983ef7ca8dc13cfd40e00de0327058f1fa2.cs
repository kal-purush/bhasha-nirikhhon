using Microsoft.AspNetCore.Mvc.Rendering;

namespace SD_310_W22SD_Assignment.Models.ViewModels
{
    public class ArtistSelectViewModel
    {
            public List<SelectListItem> ArtistSelectItems { get; set; }
            public List<Artist> Artists { get; set; }
            public List<Song> Songs { get; set; }
            public ArtistSelectViewModel(List<Artist> artists, List<Song> songs)
            {
                Artists = artists;

                ArtistSelectItems = new List<SelectListItem>();
                Artists.ForEach(a =>
                {
                    ArtistSelectItems.Add(new SelectListItem(a.Name, a.Id.ToString()));
                });

                Songs = songs;
            }
    }
}