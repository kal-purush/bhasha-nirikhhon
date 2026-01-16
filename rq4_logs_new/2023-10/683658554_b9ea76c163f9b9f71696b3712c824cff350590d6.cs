using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace YourScheduler.BusinessLogic.Models.DTOs
{
    public class RegisterUserDTO
    {
        [Required(ErrorMessage = "Email is required")]
        [EmailAddress(ErrorMessage = "Invalid Email address")]
        public string Email { get; set; } = null!;
        [Required(ErrorMessage = "Password is required")]
        [RegularExpression("(?=[A-Za-z0-9]+$)^(?=.*[a-z])(?=.*[A-Z])(?=.*[0-9])(?=.{8,}).*$", 
            ErrorMessage = "Password must contain at least on Capital letter, one small letter, one number and must be at least 8 characters")]
        public string Password { get; set; } = null!;

        [Required(ErrorMessage = "Confirm Password is required")]
        public string ConfirmPassword { get; set; } = null!;

        [Required(ErrorMessage = "DisplayName is required")]
        [MinLength(3, ErrorMessage = "DisplayName should have at least 3 characters")]
        public string DisplayName { get; set; } = null!;
    }
}