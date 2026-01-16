using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Final_Project_POO
{
    class Course
    {
        private string courseName;
        private string content;
        private string courseTeacher;
        private string courseDay;
        private string startingHour;
        private string endingHour;
        public string classroom { get; set; }

        public Course(string CourseName, string Content, string CourseTeacher, string CourseDay, string StartingHour, string EndingHour, string Classroom)
        {
            this.courseName = CourseName;
            this.content = Content;
            this.courseDay = CourseDay.ToLower();
            this.courseTeacher = CourseTeacher;
            this.classroom = Classroom;
            if (StartingHour.Length == 4)
            {
                this.startingHour = "0" + StartingHour;
            }
            else
            {
                this.startingHour = StartingHour;
            }
            if (EndingHour.Length == 4)
            {
                this.endingHour = "0" + EndingHour;
            }
            else
            {
                this.endingHour = EndingHour;
            }
        }


        public string CourseName
        {
            get { return courseName; }
            set { courseName = value; }
        }
        public string CourseDay
        {
            get { return courseDay; }
            set { courseDay = value.ToLower(); }
        }
        public string StartingHour
        {
            get { return startingHour; }
            set
            {
                if (value.Length == 4)
                    startingHour = "0" + value;
                else
                    startingHour = value;
            }
        }
        public string EndingHour
        {
            get { return endingHour; }
            set
            {
                if (value.Length == 4)
                    endingHour = "0" + value;
                else
                    endingHour = value;
            }
        }
        public string Content
        {
            get { return content; }
            set { content = value; }
        }
        public string CourseTeacher
        {
            get { return courseTeacher; }
            set { courseTeacher = value; }
        }

        public string ToStringTimetable()
        {
            return "The course " + courseName + " is taking place every " + CourseDay + " between " + startingHour + " and " + endingHour + " and your teacher is " + courseTeacher;
        }

    }
}