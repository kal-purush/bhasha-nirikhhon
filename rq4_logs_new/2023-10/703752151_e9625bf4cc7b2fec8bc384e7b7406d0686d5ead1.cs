using Microsoft.AspNetCore.Identity;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics.Tracing;

namespace Homies.Data.Models
{
    public class Organiser
    {
        public Organiser()
        {
            this.Id = Guid.NewGuid();
        }
        [Key]
        public Guid Id { get; set; }

        [Required]
        [StringLength(20, MinimumLength = 4)]
        public string Name { get; set; } = null!;

        [Required]
        [StringLength(10)]
        public string PhoneNumber { get; set; } = null!;

        [Required]
        public string UserId { get; set; }

        [Required]
        [ForeignKey(nameof(UserId))]
        public virtual IdentityUser User { get; set; } = null!;

        public virtual ICollection<Event> Events { get; set; } = new HashSet<Event>();
    }
}