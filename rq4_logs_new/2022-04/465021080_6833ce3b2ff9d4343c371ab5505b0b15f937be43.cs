using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraduateProject.Common.DTOs.Review
{
    public class ReviewDTO
    {
        public int Id { get; set; }

        [Required]
        [StringLength(50)]
        public string FullName { get; set; }

        public string Description { get; set; }

        public float Rate { get; set; }

        [Required]
        public int ReferenceId { get; set; }

        [Required]
        public string ReferenceType { get; set; }
    }
}