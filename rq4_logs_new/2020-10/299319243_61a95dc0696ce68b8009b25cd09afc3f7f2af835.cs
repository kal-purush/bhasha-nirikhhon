// AT 10-14-20
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace CISS411_Project.Models
{
    public enum ProgressReport
    {
        E, A, B //Excees, Average, Below
    }
    public class Enrollment
    {
        public int EnrollmentId { get; set; }
        public int SwimmerId { get; set; }
        public int SessionId { get; set; }
        public Swimmer Swimmer { get; set; }
        public Session Session { get; set; }
        [DisplayFormat(NullDisplayText = "No report")]
        public ProgressReport? ProgressReport { get; set; }
    }
}