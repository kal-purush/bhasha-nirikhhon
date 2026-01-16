using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CISS411_Project.Models
{
    public class Session
    {
        public string StartDate { get; set; }
        public string EndDate { get; set; }
        public int SeatsAvailable { get; set; }
        public string StartTime { get; set; }
        public int LessonId { get; set; }
        public Lesson Lesson { get; set; }
        public int SwimmerId { get; set; }
        public Swimmer Swimmer { get; set; }
        public string CoachName { get; set; }
        public Coach Coach { get; set; }
    }
}