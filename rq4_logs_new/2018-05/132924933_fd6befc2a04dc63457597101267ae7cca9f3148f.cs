using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.ComponentModel.DataAnnotations;

namespace Openlogin.Models
{
    public class ChangePassword
    {
        [Required(ErrorMessage = "Username is required")]
        public String Username { get; set; }

        [Required(ErrorMessage = "Old Password is required")]
        [DataType(DataType.Password)]
        public String OldPassword { get; set; }

        [Required(ErrorMessage = "New Password is required")]
        [DataType(DataType.Password)]
        public String NewPassword { get; set; }

        [Compare("NewPassword", ErrorMessage = "Please confirm your password")]
        [DataType(DataType.Password)]
        public String ConfirmPassword { get; set; }
    }
}