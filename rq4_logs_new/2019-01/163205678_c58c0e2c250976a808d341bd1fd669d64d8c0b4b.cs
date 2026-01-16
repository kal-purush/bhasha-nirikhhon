using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EKlubas.Domain
{
    public class StudyExam : Entity<Guid>
    {
        public StudyExam()
        {
            StudyExamResults = new List<StudyExamAnswer>();
        }

        public string UserId { get; set; }
        public EKlubasUser User { get; set; }
        public int PassMark { get; set; }
        public int Reward { get; set; }
        public ICollection<StudyExamAnswer> StudyExamResults { get; set; }
        public DateTime CreatedTime { get; set; }
        public DateTime EndDate { get; set; }
    }
}