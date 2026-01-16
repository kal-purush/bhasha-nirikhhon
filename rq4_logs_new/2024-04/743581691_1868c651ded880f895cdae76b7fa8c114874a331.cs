using System;
using System.Collections.Generic;

namespace CourseGRPC.Models
{
    public partial class LastExam
    {
        public LastExam()
        {
            QuestionExams = new HashSet<QuestionExam>();
        }

        public int Id { get; set; }
        public int? ChapterId { get; set; }
        public int? PercentageCompleted { get; set; }
        public string? Name { get; set; }
        public int? Time { get; set; }

        public virtual Chapter? Chapter { get; set; }
        public virtual ICollection<QuestionExam> QuestionExams { get; set; }
    }
}