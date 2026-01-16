using AJS.Data.Models.Enums;
using System;
using System.ComponentModel.DataAnnotations;

namespace AJS.Data.Models
{
    /// <summary>
    /// News Data Model
    /// </summary>
    public class News
    {
        [Required]
        public string NewsId { get; set; }

        [Required]
        [StringLength(DataConstants.NewsTitleMaximumLength, MinimumLength = DataConstants.NewsTitleMinimumLength)]
        public string Title { get; set; }

        [Required]
        [StringLength(DataConstants.NewsDescriptionMaximumLength, MinimumLength = DataConstants.NewsDescriptionMinimumLength)]
        public string Description { get; set; }

        [Required]
        public DateTime PublicationDate { get; set; }

        [Required]
        public NewsCategory Category { get; set; }

        [Required]
        public NewsLocation Location { get; set; }

        [Required]
        public string CreatorId { get; set; }

        public User Creator { get; set; }
    }
}