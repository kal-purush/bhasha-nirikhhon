using easyGrading.Models;
using easyGrading.Services.Interface;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace easyGrading.Services
{
    public class ClassesServices: IClassesServices
    {
        private IDbQueries _dbQueries;
        public ClassesServices(IDbQueries dbQueries) {
            _dbQueries = dbQueries;
        }

        public IEnumerable<ClassInfoViewModel> GetClassInfo(int studentId) {
            var classes = new List<ClassInfoViewModel>();
            try
            {
                var studentTakesCourses = _dbQueries.GetStudentCourses(studentId);

                foreach (var course in studentTakesCourses)
                {
                    var studentClass = new ClassInfoViewModel();

                    var courseInfo = _dbQueries.GetCourseInfo(course.Course_Id);

                    var professorInfo = _dbQueries.GetProfessorInfo(courseInfo.Prof_Id.Value);

                    if (courseInfo.Name != null && professorInfo.Name != null) {
                        var courseName = courseInfo.Name.Split(";");

                        studentClass.UserId = studentId;
                        studentClass.ClassCodeName = courseName[0];
                        studentClass.ClassName = courseName[1];
                        studentClass.Instructor = professorInfo.Name;

                    }

                    classes.Add(studentClass);

                }

                return classes;
            }
            catch (Exception e) {
                Console.WriteLine(e);
                return classes;
            }
            
        }
    }
}