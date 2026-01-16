using System;
using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Identity;

namespace Snack.Data.DataModels
{
    public class ApplicationUser : IdentityUser
    {
        [Required]
        public string FirstName { get; set; }
        [Required]
        public string LastName { get; set; }
        public DateTime DateOfBirth { get; set; }
        public int Gender { get; set; }
        [Required]
        public DateTime DateJoined { get; set; }

        public Guid ComapnyRoleId { get; set; } // current position in the company; navigation property
        public virtual CompanyRole CurrentRole { get; set; }
    }
}