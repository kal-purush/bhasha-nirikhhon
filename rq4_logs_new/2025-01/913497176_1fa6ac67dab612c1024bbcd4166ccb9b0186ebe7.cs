using Microsoft.AspNetCore.Mvc.Rendering;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel;
using LibraryWebApp.Models;
using Microsoft.EntityFrameworkCore;

namespace LibraryWebApp.ViewModels.BookViewModels
{
    public class BookVM
    {
        [Key]
        public int Id { get; set; }
        [RegularExpression(@"^[^\/\\:\*\<>\|]+$", ErrorMessage = "The Title contains invalid characters.")]
        public string Title { get; set; }

        [DisplayName("Publication Date")]
        public DateOnly PublicationDate { get; set; }

        [DisplayName("Total Count")]
        public int TotalCount { get; set; }

        [DisplayName("Cover Image")]
        public string? CoverImageUrl { get; set; }

        public string DefaultCoverImageUrl { get; set; }

        [DisplayName("Authors")]
        public IEnumerable<int> SelectedAuthorIDs { get; set; }

        public MultiSelectList? AvailableAuthors { get; set; }

        [DisplayName("Genres")]
        public IEnumerable<int> SelectedGenreIDs { get; set; }

        public MultiSelectList? AvailableGenres { get; set; }
    }
}