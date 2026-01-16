using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SISApp.Entities;
//using SISApp.DAO;

namespace SISApp.Main
{
    public class MainSIS
    {
        static void Main(string[] args)
        {
            Students student1 = new Students
            { 
                StudentID = 101,
                FirstName = "John",
                LastName = "Doe",
                DateOfBirth = new DateTime(2000, 5, 15),
                Email = "john.doe@example.com",
                PhoneNumber = "123-456-7890"
            };
            Courses course1 = new Courses
            { 
                CourseID = 201,
                CourseName = "Intro to C#",
                CourseCode = "CS101",
                InstructorName = "Dr. Smith" 
            };
            Teacher teacher1 = new Teacher
            { 
                TeacherID = 301,
                FirstName = "Sarah",
                
                LastName = "Connor",
                Email = "sarah.connor@example.com"
            };

            //Console.WriteLine($"ID: {student1.StudentID}, Name: {student1.FirstName} {LastName}, DOB: {DateOfBirth.ToShortDateString()}, Email: {Email}, Phone: {PhoneNumber}");
            Console.WriteLine($"ID: {student1.StudentID}, Name: {student1.FirstName} {student1.LastName}, DOB: {student1.DateOfBirth.ToShortDateString()}, Email: {student1.Email}, Phone: {student1.PhoneNumber}");

            //SIS sis = new SIS();

            //sis.EnrollStudentInCourse(student1, course1);
            //sis.AssignTeacherToCourse(teacher1, course1);
            //sis.RecordPayment(student1, 500.00m, DateTime.Now);

            //student1.DisplayStudentInfo();
            //course1.DisplayCourseInfo();
            //teacher1.DisplayTeacherInfo();

            //sis.GenerateEnrollmentReport(course1);
            //sis.GeneratePaymentReport(student1);
            //sis.CalculateCourseStatistics(course1);

            Console.ReadLine();
        }
    }
}