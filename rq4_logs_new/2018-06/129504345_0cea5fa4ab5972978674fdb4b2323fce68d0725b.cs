using softwareproject.Models;
using softwareproject.ViewModel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace softwareproject.Controllers
{
    public class JiraguideController : Controller
    {
        private SchoolContext db = new SchoolContext(); 
        //
        // GET: /Jiraguide/
        public ActionResult Jira()
        {
            var students =db.Students.ToList();

            return View(students);
            //Course math = new Course();
            //math.courseName = "Mathematics 909";
            //math.totalcredit = 5;
            
            //Student tayo = new Student();
            //tayo.firstName = "Tayo";
            //tayo.lastName = "Adigun";

            //Student seun = new Student();
            //seun.firstName = "seun";
            //seun.lastName = "Balogun";

            //Student john = new Student();
            //john.firstName = "Shakiru";
            //john.lastName = "Pops";

            //List<Student> studentlist = new List<Student>();
            //studentlist.Add(tayo);
            //studentlist.Add(seun);
            //studentlist.Add(john);

            //Course_Student obj = new Course_Student();
            //obj.courses = math;
            //obj.students = studentlist;
           
        }

        public ActionResult Index() {

            return View(); 
        }
	}
}