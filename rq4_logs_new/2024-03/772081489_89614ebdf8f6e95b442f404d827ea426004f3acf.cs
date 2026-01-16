using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Examination.Models
{
    
    public class StudentCourse
    {
        public int StudentID { get; set; }
        public int CourseID { get; set; }
        public string CourseName { get; set; }

        [Key]
        [Column(Order = 1)]
        public int SId { get; set; }

        [Key]
        [Column(Order = 2)]
        public int CrId { get; set; }

        public int Stgrade { get; set; }

        

    }
}

