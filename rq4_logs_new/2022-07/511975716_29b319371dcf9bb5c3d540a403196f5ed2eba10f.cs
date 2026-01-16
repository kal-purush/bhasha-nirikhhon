using Microsoft.AspNetCore.Mvc;
using NimmalaWeek2R.Models;


namespace NimmalaWeek2R.Controllers
{
    public class HomeController : Controller
    {
        [HttpGet]//Method Attribute
        //when the page runs for the first time, returns the index
        //view with the hello model
        public IActionResult Index()
        {
            return View(new HelloModel());
        }// end Index()

        [HttpPost]
        public IActionResult Index(HelloModel helloModel)
        {
            return View("Hello",helloModel);
        }
    }//controller
}