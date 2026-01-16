using System;
using System.ComponentModel.DataAnnotations;

namespace flight_api.Models
{
	public class Login
	{
        [Required(ErrorMessage = "Email is required")]
        [EmailAddress(ErrorMessage = "Invalid email format")]
        public string Email { get; set; }

        [Required(ErrorMessage = "Password is required")]
        public string Password { get; set; }
    }
}
