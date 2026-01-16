using System.ComponentModel.DataAnnotations;

namespace PortfolioProject.Models
{
    /// <summary>
    /// Represents a singular bookmark for users to refer back to
    /// </summary>
    public class Bookmark
    {
        /// <summary>
        /// Uniquely identifies each bookmark in the database
        /// </summary>
        [Key]
        public int BookmarkId { get; set; }

        /// <summary>
        /// The user associated with the bookmark
        /// </summary>
        [Required]
        public string UserId { get; set; }

        /// <summary>
        /// The URL that the bookmark brings a user to when
        /// clicked on
        /// </summary>
        [Required]
        public string BookmarkURL { get; set; }

        /// <summary>
        /// The name of the page the bookmarks brings a 
        /// user to when clicked on
        /// </summary>
        [Required]
        public string BookmarkName { get; set; }
    }
}