using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;

namespace SPSS.Entities
{
    public class Address
    {
        [Key]
        public int Id { get; set; }

        [Required]
        [MaxLength(155)]
        public string AddressLine1 { get; set; } = string.Empty;

        [MaxLength(155)]
        public string? AddressLine2 { get; set; }

        [ForeignKey("City")]
        public int CityId { get; set; }
        public virtual City? City { get; set; }

        [ForeignKey("AddressType")]
        public int AddressTypeId { get; set; }
        public virtual AddressType? AddressType { get; set; }
        public virtual ICollection<UserAddress> UserAddresses { get; set; } = new List<UserAddress>();

    }
}