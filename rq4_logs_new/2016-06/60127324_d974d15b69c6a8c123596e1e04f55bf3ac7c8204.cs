using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using ToyWebsite.DAL;
using ToyWebsite.Models;

namespace ToyWebsite.Controllers
{
    public class AccountController : Controller
    {
        
        public ActionResult Register()
        {
            return View();
        }

        [HttpPost]
        public ActionResult AddUser(string aUserName, string aUserPassword, string aUserEmail)
        {

            return RedirectToAction("RegistrationComplete");
        }

        public ActionResult RegistrationComplete()
        {
            return View();
        }


        


    }
}