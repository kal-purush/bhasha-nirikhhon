using Microsoft.AspNetCore.Mvc;

using System.Diagnostics;
// Created by Nimmala
//111111111111
namespace NimmalaWeek3.Controllers

{
    public class HomeController : Controller
    {



        public IActionResult Index()
        {
            return View();
            //Index()
        }
        [HttpPost]
        public IActionResult Index(int number)
        {
            //In this simple binding example, where there is no model defined, note that the name property of the html text box matches with the name 
            //of the incoming parameter number.

            if (ModelState.IsValid)
            {
                int result = (int)Math.Pow(number, 2);
                ViewBag.Result = $"The square of the number you entered{number} is {result}";
            }
            else
            {
                ViewBag.Result = $"Please enter a valid number as input";
            }
          

            return View();

        }// Index(number)

        [HttpPost]
        public IActionResult WithName(double number, string name)
        {
            if(ModelState.IsValid)
            {
               
                string fortune = "";
                switch(number)
                {
                    case 1:
                        fortune = "You are number 1";
                        break;
                        case 2: 
                        fortune = "You are number 2";
                        break ;
                        case 3:     
                        fortune = "You are number 3";
                        break;
                        case 4:
                        fortune = "You are number 4";
                        break;
                        case 5:
                        fortune = "You are number 5";
                        break;
                        case 6:
                        fortune = "You are number 6";
                        break;
                        case 7:
                        fortune = "You are number 7";
                        break;
                        default:
                        fortune="<p> You are no number</P>";
                        break;
                }//Switch
                ViewBag.Result=$"Hello{name}.You entered{number}.Here is the fortune Quote for you:\r\n{fortune}";

            }//model Validation if
            else
            {
                ViewBag.Result = "Invalid Input!Try again";

            }//model validation if/else
            return View("Index");
        }// Index(number, name)
    
    }       // Controller Class


}// namespace