using CISS411_Project.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CISS411_Project.ViewModels
{
    public class CoachLessonViewModel
    {
        public IEnumerable<Lesson> Lessons { get; set; }
        public Coach Coach { get; set; }
    }
}