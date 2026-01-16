using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Frontend.Models;

namespace Frontend.Controllers
{
    public class SelectEquipmentController : Controller
    {
        private readonly ILogger<SelectEquipmentController> _logger;

        public SelectEquipmentController(ILogger<SelectEquipmentController> logger)
        {
            _logger = logger;
        }

        public IActionResult Index()
        {
            
            return View();
        }


    }
}