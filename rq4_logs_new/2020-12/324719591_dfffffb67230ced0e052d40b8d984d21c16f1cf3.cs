using System.ComponentModel.DataAnnotations;

namespace HaloBiz.DTOs.ReceivingDTOs
{
    public class OfficeReceivingDTO
    {
        [Required, MinLength(3), MaxLength(50)]
        public string Name { get; set; }
        [Required, MinLength(3), MaxLength(255)]
        public string Description { get; set; }
        [Required]
        public long StateId { get; set; }
        [Required]
        public long LGAId { get; set; }
        [Required]
        public string Street { get; set; }
        public string PhoneNumber { get; set; }
        public long HeadId { get; set; }
        public long BranchId { get; set; }
    }
}