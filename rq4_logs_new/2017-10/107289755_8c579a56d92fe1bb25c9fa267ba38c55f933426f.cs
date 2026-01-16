using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace lab26Identity.Models
{
    public class LoginViewModel
    {
        [Required]
        [Display(Name = "UserName")]
        [StringLength(20, MinimumLength = 6)]
        public string UserName { get; set; }

        [Required]
        [StringLength(100, ErrorMessage = "Does not meet password requirements", MinimumLength = 8)]
        [DataType(DataType.Password)]
        [Display(Name = "Password")]
        public string Password { get; set; }

        [Display(Name = "Remember Me")]
        public bool RememberMe { get; set; }
    }
}