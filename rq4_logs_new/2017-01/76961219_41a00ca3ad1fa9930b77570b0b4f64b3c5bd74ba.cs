using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace Trackify.Controllers
{
    public class HomeController : Controller
    {
        public IActionResult Index()
        {
            UserAdapter.GetUserById(1);
            return View();
        }

        public IActionResult About()
        {
            User ali = UserAdapter.GetUserById(1);
            ViewData["Message"] = "USER:"+ ali.UserName;

            return View();
        }
            
        public IActionResult Contact()
        {
            ViewData["Message"] = "Your contact page.";

            return View();
        }

        public IActionResult Error()
        {
            return View();
        }
    }
}