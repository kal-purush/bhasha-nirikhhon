using System;
using System.Collections.Generic;
using System.Linq;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace eyesonthenet.Models
{
    public class Users
    {
        [Key]
        public int UserId { get; set; }

        [Required, MaxLength(16)]
        public string Username { get; set; }

        [Required, MinLength(8)]
        public string Password { get; set; }

        [Required]
        public string Email { get; set; }

        public DateTime RegDate { get; set; }

        public List<Cameras> CameraList { get; set; }
    }
}