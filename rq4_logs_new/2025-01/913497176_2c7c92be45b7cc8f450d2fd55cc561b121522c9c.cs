using Microsoft.AspNetCore.Mvc.Rendering;

namespace LibraryWebApp.ViewModels
{
    public class HomeViewModel
    {
        public IEnumerable<BookSimplified> BookData { get; set; }
        public int Page { get; set; }
        public int MaxPage { get; set; }
        public string? SearchTerm { get; set; }
        public IEnumerable<SelectListItem> SearchByDropDown { get; set; }
        public string? SearchBy { get; set; }

        public IEnumerable<SelectListItem> OrderByDropDown { get; set; }
        public string? OrderBy { get; set; }

    }
}