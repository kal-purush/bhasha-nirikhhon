using Examination.Models;
using System.ComponentModel.DataAnnotations;

namespace Examination.ViewModel
{
    public class StudentCourseTrackModel
    {

        [Key]
        public int SId { get; set; }
        [Required(ErrorMessage = " Name is Required")]
        [StringLength(20, MinimumLength = 3)]
        [RegularExpression(@"^[a-zA-Z\s]*$", ErrorMessage = "Invalid Name")]
        public string Sname { get; set; }

        [Required(ErrorMessage = "Email is Required")]
        [RegularExpression(@"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,63}$", ErrorMessage = "Invalid Email")]
        public string Semail { get; set; }

        [Required(ErrorMessage = " Passwprd is Required")]
        [RegularExpression(@"^(?=.*\d)(?=.*[a-z])(?=.*[A-Z]).{6,12}$", ErrorMessage = "Password must be 6-12 characters long and include at least one digit, one uppercase letter, and one lowercase letter.")]
        public string password { get; set; }

        [Required(ErrorMessage = " Gender is Required")]
        public string Sgender { get; set; }

        [Required(ErrorMessage = " track is Required")]
        public int TrackId { get; set; }

        public virtual ICollection<Student_Answer> Student_Answers { get; set; } = new List<Student_Answer>();

        public virtual ICollection<Student_Course> Student_Courses { get; set; } = new List<Student_Course>();

        [Required(ErrorMessage = " track is Required")]
        public string TrackName { get; set; }
        [Required(ErrorMessage = " course is Required")]
        public string CourseName { get; set; }

        [Required(ErrorMessage = " course is Required")]
        public int CrsId { get; set; }

        public virtual Track Track { get; set; }
        public virtual ICollection<Course> Crs { get; set; } = new List<Course>();
        public virtual ICollection<StudentCourse> Courses { get; set; }
    }
}