using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;


namespace Darkec.Models
{
    public class Register
    {
        [Key]
        public int UserId { get; set; }

        [Required(ErrorMessage = "Please Enter Username..")]
        [Display(Name = "UserName")]
        public string Username { get; set; }

        [Required(ErrorMessage = "Please Enter Password...")]
        [DataType(DataType.Password)]
        [Display(Name = "Password")]
        public string Password { get; set; }

        //[Required(ErrorMessage = "Please Enter the Confirm Password...")]
        //[DataType(DataType.Password)]
        //[Display(Name = "Confirm Password")]
        //[Compare("Password")]
        //public string ConfirmPassword { get; set; }

        // ^^ Since we've got no database we may leave the register form without a password confirmation, idk 

        [Required(ErrorMessage = "Please Enter Email...")]
        [Display(Name = "Email")]
        public string Email { get; set; }




    }
}