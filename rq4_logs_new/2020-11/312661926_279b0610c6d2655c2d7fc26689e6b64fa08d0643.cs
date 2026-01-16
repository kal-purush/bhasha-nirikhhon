using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace Snack.Dto.Requests
{
    public class UpdateUserProfileRequest
    {
        [Required]
        public string Email { get; set; }
        [Required]
        public string UserName { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        [Phone]
        public string PhoneNumber { get; set; }
        public DateTime DateOfBirth { get; set; }
        public int Gender { get; set; }
        [Required]
        public Guid Designation { get; set; }
        public List<string> AddRoles { get; set; }
        public List<string> RemoveRoles { get; set; }
    }
}