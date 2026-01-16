using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LanguageSchoolApp.model.Courses
{
    public class FinishedCourseDTO
    {
        public int CourseId { get; set; }
        public LanguageProficiency LanguageProficiency { get; set; }
        public int Duration { get; set; }
        public List<ClassPeriod> ClassPeriods { get; set; }
        public DateTime BeginningDate { get; set; }
        public CourseType CourseType { get; set; }
        public int Grade { get; set; }

        public FinishedCourseDTO() { }

        public FinishedCourseDTO(int _id, LanguageProficiency _languageProficiency, int _duration, List<ClassPeriod> _classPeriods, DateTime _beginningDate, CourseType _courseType, int _grade)
        {
            CourseId = _id;
            LanguageProficiency = _languageProficiency;
            Duration = _duration;
            ClassPeriods = _classPeriods;
            BeginningDate = _beginningDate;
            CourseType = _courseType;
            Grade = _grade;
        }

        public string ClassPeriodsToString()
        {
            return ClassPeriods.Count + "days/w";
        }
    }
}