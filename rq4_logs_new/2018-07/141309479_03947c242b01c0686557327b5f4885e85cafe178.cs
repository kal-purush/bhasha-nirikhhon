using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using UniversityRegistrar.Models;


namespace UniversityRegistrar.Controllers
{
    public class CoursesController : Controller
    {
        [HttpGet("/courses")]
        public ActionResult Index()
        {
            List<Course> allCourses = Course.GetAll();
            return View(allCourses);
        }

        [HttpPost("/courses")]
        public ActionResult Index(string name, string number)
        {
            Course newCourse = new Course(name, number);
            newCourse.Save();
            return RedirectToAction("Index");
        }

        [HttpGet("/courses/add")]
        public ActionResult AddForm()
        {
            return View();
        }

        [HttpPost("/courses/add")]
        public ActionResult Add()
        {
            return View();
        }
    }
}