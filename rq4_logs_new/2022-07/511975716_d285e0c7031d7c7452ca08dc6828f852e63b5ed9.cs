using ExamPractise.Models;
using Microsoft.AspNetCore.Mvc;

namespace ExamPractise.Controllers
{
    public class HomeController : Controller
    {
        
        [HttpGet]
        public ViewResult Index()
        {
            return View(new BillModel());
        }
        [HttpPost]
        public IActionResult Index(BillModel billModel)
        {
            if (ModelState.IsValid)
            {
                return View("Confirm", billModel);
            }
            return Redirect("/");
        }
    
        
    }
}