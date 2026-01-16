using System;
using System.ComponentModel.DataAnnotations;

namespace NeutrackAPI.DTOs
{
    public class UserPatchDTO
    {
        [MaxLength(60)]
        [RegularExpression(@"^([\w\.\-]+)@([\w\-]+)((\.(\w){2,3})+)$",
            ErrorMessage = "Email is invalid")]
        public string Email { get; set; }

        [MaxLength(256)]
        public string Password { get; set; }

        [MaxLength(100)]
        public string FirstName { get; set; }

        [MaxLength(100)]
        public string LastName { get; set; }

        public string PhoneNumber { get; set; }

        [MaxLength(50)]
        public string Gender { get; set; }

        public DateTime DateOfBirth { get; set; }

        public string Goal { get; set; }

        public int ActivityLevel { get; set; }

        public double Height { get; set; }

        public double Weight { get; set; }

        public bool IsActive { get; set; }

        public int YearsOfExperience { get; set; }

    }
}