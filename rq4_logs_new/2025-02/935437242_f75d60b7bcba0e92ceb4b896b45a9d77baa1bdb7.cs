using System.ComponentModel.DataAnnotations;

namespace PortfolioProject.Models
{
    /// <summary>
    /// Represents a singular section
    /// </summary>
    public class Section
    {
        /// <summary>
        /// Uniquely identifies a section
        /// </summary>
        [Key]
        public string SectionId { get; set; }

        /// <summary>
        /// The internal name of the section, will help provide
        /// a working clickable link when displayed on a webpage
        /// </summary>
        [Required]
        public string SectionName { get; set; }

        /// <summary>
        /// The name of the section that is displayed on
        /// a webpage
        /// </summary>
        [Required]
        public string SectionDisplay { get; set; }
    }
}