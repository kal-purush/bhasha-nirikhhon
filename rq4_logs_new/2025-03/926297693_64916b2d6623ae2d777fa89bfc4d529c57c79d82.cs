using SPSS.Entities;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SPSS.Repository.Entities
{
    public class BookingInfo
    {
        [Key]
        public int Id { get; set; }

        [Required]
        [ForeignKey("Customer")]
        public string CustomerId { get; set; } = string.Empty;

        [Required]
        [ForeignKey("Expert")]
        public string ExpertId { get; set; } = string.Empty;

        [Required]
        public DateTime CreateDate { get; set; } = DateTime.UtcNow;

        [Required]
        public DateTime BookingDate { get; set; }

        [Required]
        public TimeSpan Start_time { get; set; }

        [Required]
        public TimeSpan End_time { get; set; }

        [MaxLength(500)]
        public string? Special_requests { get; set; }

        [Required]
        public int Status { get; set; } // Có thể sử dụng Enum

        public bool isDelete { get; set; } = false;

        // Navigation properties
        public virtual AppUser? Customer { get; set; }
        public virtual AppUser? Expert { get; set; }
    }
}