using System.ComponentModel.DataAnnotations;

namespace WebShopLibrary.BusinessAccessLayer.DTOs.MonitorDTO
{
    public class MonitorRequest
    {
        [Required]
        [StringLength(100, ErrorMessage = "Language can not be longer than 100 characters long")]
        public string Brand { get; set; }
        [Required]
        public int Size { get; set; }
        [Required]
        public int ReleaseYear { get; set; }
        [Required]
        public int ProductId { get; set; }
        [Required]
        public int CategoryId { get; set; }
    }
}