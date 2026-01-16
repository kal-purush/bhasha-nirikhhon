using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ACTApp.Models.domain
{
    public class MathModel
    {
        [Required]
        public int UserId { get; set; }
        [Required]
        public string Easy { get; set; }
        public string Medium { get; set; }
        public string Hard { get; set; }
    }
}