using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SigaaWindows.Adapter
{
    public class CourseAdapter
    {
        public Model.Course GetCourse(Model.API.CourseResult courseResult)
        {
            return new Model.Course
            {
                ID = courseResult.id,
                Title = courseResult.title
            };
        }

        public IEnumerable<Model.Course> GetCourses(IEnumerable<Model.API.CourseResult> courseResults)
        {
            List<Model.Course> courses = new List<Model.Course>();
            foreach(Model.API.CourseResult courseResult in courseResults)
            {
                courses.Add(new Model.Course
                {
                    ID = courseResult.id,
                    Title = courseResult.title
                });
            }
            return courses;
        }
    }
}