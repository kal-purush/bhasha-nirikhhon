using System;
using System.Collections.Generic;
using System.Text;
using CourseGenerator.Models.Entities.Identity;
using CourseGenerator.Models.Entities.InfoByThemes;
using CourseGenerator.Models.Entities.Info;

namespace CourseGenerator.Models.Entities.CourseAccess
{
    public class UserCourse
    {
        public int UserId { get; set; }
        public User User { get; set; }

        public int CourseId { get; set; }
        public Course Course { get; set; }

        public int LevelId { get; set; }
        public Level Level { get; set; }

        public string Note { get; set; }
    }
}