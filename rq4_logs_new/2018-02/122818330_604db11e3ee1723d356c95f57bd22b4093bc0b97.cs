using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using School.Entities;
using School.Business.Interfaces;
using School.DataAccess.Interfaces;
using System.Data;

namespace School.Business.Implementations
{
    public class CourseBusiness: ICourseBusiness
    {
        ICourseDataAccess _dataAccess;

        public CourseBusiness(ICourseDataAccess dataAccess)
        {
            _dataAccess = dataAccess;
        }

        /// <summary>
        /// This method will receive the data in the form of a DataTable returned
        /// by the DataAccess. It is supposed to iterate through each row in the dataTable
        /// and create a new Course entity and set all the properties with their respective value
        /// extracted from the row being iterated. Once all the properties is set this entity is 
        /// then supposed ro be added to the list of courses and returned to the caller.
        /// </summary>
        /// <returns>List of Courses found in the Course table</returns>
        public List<Course> GetAllCourses() {                        
            List<Course> courses = new List<Course>();
            foreach (DataRow row in _dataAccess.GetAllCourses().Rows)
            {
                Course course = new Course();
                course.ID = Convert.ToInt32(row["ID"].ToString());
                course.CourseNumber = row["CourseNumber"].ToString();
                course.CourseName = row["CourseName"].ToString();
                course.CourseDescription = row["CourseDescription"].ToString();
                course.Credits = Convert.ToInt32(row["Credits"].ToString());
                course.Department.DepartmentCode = row["DepartmentCode"].ToString();
                course.Department.DepartmentDescription = row["DepartmentDescription"].ToString();
                courses.Add(course);
            }
            return courses;
        }

        /// <summary>
        /// This method will receive the data in the form of a DataTable returned
        /// by the DataAccess. It is supposed to iterate through each row in the dataTable
        /// and create a new Course entity and set all the properties with their respective value
        /// extracted from the row being iterated. Once all the properties is set this entity is 
        /// then supposed ro be added to the list of courses and returned to the caller.
        /// </summary>
        /// <param name="CourseName">Some chacaters that the course name may contain</param>
        /// <returns>List of All possible courses whose name may contain the charaters provided in the CourseName</returns>
        public List<Course> GetAllCoursesByCourseName(string CourseName) {
            throw new NotImplementedException();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="ID"></param>
        /// <returns></returns>
        public Course GetCourseByID(int ID)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Course to be added
        /// </summary>
        /// <param name="course">Course to be added</param>
        /// <returns>Success= true if no error else false</returns>
        public bool AddCourse(Course course)
        {
            return _dataAccess.AddCourse(course.CourseNumber, course.CourseName, course.CourseDescription, course.Credits, course.Department.ID);
        }

        /// <summary>
        /// Adds a course to database
        /// </summary>
        /// <param name="course">Course to be updated</param>
        /// <returns>Success= true if no error else false</returns>
        public bool UpdateCourse(Course course)
        {
            return _dataAccess.UpdateCourse(course.ID,course.CourseNumber,course.CourseName,course.CourseDescription,course.Credits);
        }

        /// <summary>
        /// Deletes a course found at the ID provieded
        /// </summary>
        /// <param name="Id"></param>
        /// <returns></returns>
        public bool DeleteCourse(int Id)
        {
            throw new NotImplementedException();
        }
    }
}