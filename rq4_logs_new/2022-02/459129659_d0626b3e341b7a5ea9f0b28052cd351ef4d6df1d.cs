using System;
using System.ComponentModel.DataAnnotations;

namespace WebShopLibrary.BusinessAccessLayer.DTOs.GameDTO
{
    public class GameRequest
    {
        [Required]
        [StringLength(50, ErrorMessage = "Publisher can not be longer than 50 characters long")]
        public string Publisher { get; set; }

        [Required]
        [Range(1900, 3000, ErrorMessage = "Year of publishing must be between 1900 and 3000")]
        public int PublishedYear { get; set; }

        [Required]
        [StringLength(32, ErrorMessage = "Language can not be longer than 32 characters long")]
        public string Language { get; set; }

        [Required]
        [StringLength(32, ErrorMessage = "Genre can not be longer than 32 characters long")]
        public string Genre { get; set; }

        [Required]
        [Range(1, 1000000, ErrorMessage = "Product ID must be between 1 and 1.000.000")]
        public int ProductId { get; set; }

        [Required]
        [Range(1, 1000000, ErrorMessage = "Category ID must be between 1 and 1.000.000")]
        public int CategoryId { get; set; }
    }
}