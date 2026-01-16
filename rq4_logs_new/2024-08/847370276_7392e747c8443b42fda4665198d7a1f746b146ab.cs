using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using PROGRA3.Models;


namespace PROGRA3.Controllers
{
    public class BolsaController : Controller
    {
        private readonly ILogger<BolsaController> _logger;

        public BolsaController(ILogger<BolsaController> logger)
        {
            _logger = logger;
        }

        public IActionResult Index()
        {
            return View();
        }
        
        [HttpPost]
        public IActionResult Ordenar(Bolsa bolsas)
        {
           
                // Calcula el total, IGV y cobro
                bolsas = new Bolsa(
                    bolsas.Nombres, 
                    bolsas.Correo, 
                    bolsas.FecOpe, 
                    bolsas.Sp, 
                    bolsas.Dj, 
                    bolsas.Bu, 
                    bolsas.MontoAbonar
                );

                List<Bolsa> listaBolsa = new List<Bolsa> { bolsas };
                ViewData["listaBolsa"] = listaBolsa;

                return View("Index", bolsas);
    
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View("Error!");
        }
    }
}