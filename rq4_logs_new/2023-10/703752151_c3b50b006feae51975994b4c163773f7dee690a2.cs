using Microsoft.AspNetCore.Identity;
using System.ComponentModel.DataAnnotations;

namespace Homies.Data.Models
{
    public class ApplicationUser : IdentityUser<Guid>
    {
        public ApplicationUser()
        {
            Id = Guid.NewGuid();
        }

        [Required]
        [MaxLength(50)]
        public string FirstName { get; set; } = null!;

        [Required]
        [MaxLength(50)]
        public string LastName { get; set; } = null!;

        public virtual ICollection<Event> Events { get; set; } = new HashSet<Event>();
    }
}