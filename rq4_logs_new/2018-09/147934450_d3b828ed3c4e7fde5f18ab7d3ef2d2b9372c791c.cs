using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace Tapir.Models
{
    public class Company
    {
        public int ID { get; set; }
        [Required]
        [StringLength(100, ErrorMessage = "Company full name length cannot exceed 100 characters.")]
        public string FullName { get; set; }
        [Required]
        [StringLength(20, ErrorMessage = "Company short name length cannot exceed 20 characters.")]
        public string ShortName { get; set; }
        public ICollection<Employment> Employments { get; set; }        
    }
}