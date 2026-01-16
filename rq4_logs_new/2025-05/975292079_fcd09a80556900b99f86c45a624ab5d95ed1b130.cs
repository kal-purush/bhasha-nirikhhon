using System.ComponentModel.DataAnnotations;

namespace Libre_ERP_API.DTO_s
{
    public class CreateProductRequest
    {
        [Required]
        [Range(1, int.MaxValue, ErrorMessage = "Invalid User")]
        public int IDUser { get; set; }

        [Required]
        [StringLength(100, ErrorMessage = "Name must be less than 100 characters")]
        public string Name { get; set; }

        [Range(0, int.MaxValue, ErrorMessage = "Price must be greater than 0")]
        public decimal Price { get; set; }

        [Range(0, int.MaxValue, ErrorMessage = "MinStock must be positive")]
        public int MinStock { get; set; }
    }
}