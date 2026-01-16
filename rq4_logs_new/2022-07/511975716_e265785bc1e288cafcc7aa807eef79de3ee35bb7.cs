using Microsoft.AspNetCore.Mvc;

namespace NimmalaWeek2.Controllers
{
    public class HomeController : Controller
    {
        public IActionResult Index()
        {
            //variable for date
            string todaysDate = DateTime.Now.ToString();
            string myName = "Naveen Nimmala";
            string message = $"Welcome to {myName}";

            //Creating data for sending to the view
            ViewData["Message"] = message;
            ViewBag.TodaysDate =$"Today's Date is:{todaysDate}";
            return View();
        }//end of Index()
    }//end of controll class
}//namespace