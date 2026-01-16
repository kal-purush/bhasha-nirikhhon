using CleanArchMvc.Domain.Entities;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CleanArchMvc.Application.DTOs
{
    public class ProductDTO
    {
        public int Id { get; set; }

        [Required(ErrorMessage = "The name is required")]
        [MinLength(3, ErrorMessage = "The name is too short. minimum 3 characters")]
        [MaxLength(100, ErrorMessage = "The name is greater than 100 characters")]
        [DisplayName("Name")]
        public string Name { get; private set; }

        [Required(ErrorMessage = "The description is required")]
        [MinLength(5, ErrorMessage = "The description is too short. minimum 5 characters")]
        [MaxLength(200, ErrorMessage = "The description is greater than 200 characters")]
        [DisplayName("Description")]
        public string Description { get; private set; }

        [Required(ErrorMessage = "The price is required")]
        [Column(TypeName = "decimal(18,2)")]
        [DisplayFormat(DataFormatString = "{0:C2}")]
        [DataType(DataType.Currency)]
        [DisplayName("Price")]
        public decimal Price { get; private set; }

        [Required(ErrorMessage = "The stock is required")]
        [Range(1, 9999, ErrorMessage = "The stock is greater than 9.999")]
        [DisplayName("Stock")]
        public int Stock { get; private set; }

        [MaxLength(250, ErrorMessage = "The image name is greater than 250 characters")]
        [DisplayName("Product Image")]
        public string Image { get; private set; }

        public Category Category { get; set; }

        [DisplayName("Categories")]
        public int CategoryId { get; set; }
    }
}