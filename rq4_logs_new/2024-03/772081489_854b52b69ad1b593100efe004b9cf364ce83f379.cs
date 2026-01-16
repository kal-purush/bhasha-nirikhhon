using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Examination.ViewModel
{
    public class StudentCourseModel
    {
        [Key]
        [Column(Order = 1)]
        public int SId { get; set; }

        [Key]
        [Column(Order = 2)]
        public int CrId { get; set; }

        public int Grade { get; set; }

        [NotMapped]
        public string Sname { get; set; }

        [NotMapped]
        public int StudentId { get; set; }

        [NotMapped]
        public string Tname { get; set; }

        [NotMapped]
        public string Cname { get; set; }
    }
}