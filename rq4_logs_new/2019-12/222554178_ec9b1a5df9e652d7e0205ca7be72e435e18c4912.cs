using easyGrading.Models;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;

namespace EasyGrading.Controllers
{
    public class CalculationController : Controller
    {
        public IActionResult CalculationView()
        {

            var model = new List<CalculationModel>(){
              new CalculationModel() {
                Components = "Assignment 1",
                Status = "marked",
                Marks = "100%"
            },
              new CalculationModel() {
                Components = "Assignment 1",
                Status = "marked",
                Marks = "100%"
            }
        };
            return View(model);
        }
    }
}