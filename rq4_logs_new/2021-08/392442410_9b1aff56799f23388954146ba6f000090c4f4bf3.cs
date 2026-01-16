using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;


namespace jquery_ajax_aspnet_core.Controllers
{
  public class HomeController : Controller
  {
    public IActionResult Index()
    {
      return View();
    }

    [HttpPost]
    public int Add(int number1, int number2)
    {
      return number1 + number2;
    }

    [HttpGet]
    public Numbers Calculate(int number1, int number2)
    {
      Numbers numbers = new Numbers();
      numbers.Add = number1 + number2;
      numbers.Substract = number1 - number2;
      numbers.Multiply = number1 * number2;
      numbers.Divide = (decimal)number1 / number2;

      return numbers;
    }
  }
}