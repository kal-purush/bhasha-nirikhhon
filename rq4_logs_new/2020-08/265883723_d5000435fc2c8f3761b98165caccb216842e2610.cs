using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace ShikShaq.Controllers
{
    public class ErrorController : Controller
    {
        public IActionResult AccessDeniedPage()
        {
            return View();
        }
    }
}