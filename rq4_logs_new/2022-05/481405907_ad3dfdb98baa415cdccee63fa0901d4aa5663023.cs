using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;


namespace MyFirstApp.Models.API
{
    public class Login
    {
       
            [Required(ErrorMessage = "The Username is required!")]
            public string Username { get; set; }

            [Required(ErrorMessage = "The password is required!")]
            public string Password { get; set; }
        
    }
}