using System.ComponentModel.DataAnnotations;
using Homies.Data.Models;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace Homies.Web.ViewModels
{
    public class AddEventViewModel
    {
        [Key]
        public Guid Id { get; set; }

        [Required]
        [StringLength(20, MinimumLength = 10)]
        public string Name { get; set; } = null!;

        [Required]
        [StringLength(150, MinimumLength = 50)]
        public string Description { get; set; } = null!;

        [Required]
        public DateTime Start { get; set; }

        [Required]
        public DateTime End { get; set; }

        public int CategoryId { get; set; } 

        public IEnumerable<CategoryViewModel> Categories { get; set; } = new List<CategoryViewModel>();
    }
}