using System;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;

namespace Bulky.Models
{
    public class Category
    {
        [Key] // Explicitly defining primary key for the model
        public int Id { get; set; }
        [Required]
        [MaxLength(30)]
        [DisplayName("Category Name")]
        public string Name { get; set; }
        [DisplayName("Display Order")]
        [System.ComponentModel.DataAnnotations.Range(1, 100, ErrorMessage = "Display Order must be beween 1-100")]
        public int DisplayOrder { get; set; }
    }
}