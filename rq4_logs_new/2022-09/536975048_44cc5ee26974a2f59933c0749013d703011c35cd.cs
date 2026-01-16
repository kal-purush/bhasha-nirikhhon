using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace Exercise9.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class StudentController : ControllerBase
    {
        [HttpGet()]
        public string GetStudents(){
            //create a collection of students (3 students)
            //and return it
            List<Student> studentsCollection = new List<Student>();
            studentsCollection.Add(new Student {Fname="Jim", Lname="Jones"});
            studentsCollection.Add(new Student {Fname="Ann", Lname="Jones"});
            studentsCollection.Add(new Student {Fname="Lisa", Lname="Jones"});
            string result="";
            foreach(Student stu in studentsCollection){
                result+=stu.Fname+" "+stu.Lname+"\n";
            }
            return result;
        }
        [HttpPost]
        public string AddStudent(Student objStudent){
            return objStudent.Fname+" "+objStudent.Lname;
        }
    }
}