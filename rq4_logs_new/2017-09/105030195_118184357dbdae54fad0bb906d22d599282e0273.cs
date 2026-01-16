using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Lab11Cameron.Controllers
{
    public class HomeController : Controller
    {
        public string Index()
        {
            return "Welcome to student enrollent \n\nTo register as new student, add to your url: \n\n  /Students/NewEntry?Name=''&Major=''&Year=''" +
                "\n\nTo view all students add this to your URL:\n\n  /Students/ViewStudents?Names=''";
        }

        public string GradYear(int graduation)
        {
            return $"Graduation Year: {graduation}";
        }
    }
}