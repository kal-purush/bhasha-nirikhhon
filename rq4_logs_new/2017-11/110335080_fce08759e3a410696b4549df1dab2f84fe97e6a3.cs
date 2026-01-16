using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using MvcValidation.Models;
using System.Text.RegularExpressions;

namespace MvcValidation.Controllers
{
    public class HomeController : Controller
    {
        // GET: Home
        public ActionResult Index()
        {
            return View();
        }
        [HttpPost]
        public ActionResult Index(Employee employee)
        {
            //if (string.IsNullOrWhiteSpace(employee.Name))
            //{
            //    ModelState.AddModelError("Name", "Please Enter Name. ");
            //}
            //if (!string.IsNullOrWhiteSpace(employee.Email))
            //{
            //    string emailRegex = @"^([a-zA-Z0-9_\.\-])+\@(([a-zA-Z0-9\-])+\.)+([a-zA-Z0-9]{2,4})+$";
            //    Regex re = new Regex(emailRegex);
            //    if (!re.IsMatch(employee.Email))
            //    {
            //        ModelState.AddModelError("Email", "Please Enter Correct Email Address");

            //    }
            //}
            //else
            //{
            //    ModelState.AddModelError("Email", "Please Enter Email Address");
            //}
            //if (string.IsNullOrWhiteSpace(employee.Mobile))
            //{
            //    //Mobile is textbox id 
            //    ModelState.AddModelError("Mobile", "Please enter your mobile no");
            //}
            if (ModelState.IsValid)
            {
                return View("Index");

            }
            return View("Index");

        }
    }
}