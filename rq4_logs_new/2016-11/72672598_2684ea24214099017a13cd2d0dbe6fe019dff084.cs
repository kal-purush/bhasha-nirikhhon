using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Linq;
using System;

[Route("/")]
public class HomeController : Controller {
    
    [HttpGet]
    public IActionResult Index() => Redirect("/swagger/ui");
}