using System.ComponentModel.DataAnnotations;

namespace Project.Models
{
    public class CustomerDTO
    {
        public int Id { get; set; }
        [Required]
        public string FirstName { get; set; }
        [Required]
        public string LastName { get; set; }
        [Required]
        public string Email { get; set; }
        [Required]
        public int PhoneNumber { get; set; }
        [Required]
        public int? ShopingCartId { get; set; }
        [Required]
        public virtual ShopingCart ShopingCart { get; set; }
    }
}