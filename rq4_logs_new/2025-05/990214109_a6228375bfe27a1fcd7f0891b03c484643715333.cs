using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using apptienda.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace apptienda.Controllers
{

    public class CalculadoraController : Controller
    {
        private readonly ILogger<CalculadoraController> _logger;

        public CalculadoraController(ILogger<CalculadoraController> logger)
        {
            _logger = logger;
        }

        public IActionResult Index()
        {
            return View();
        }

        public IActionResult Calculo(Calculo calc)
        {
            if (ModelState.IsValid)
            {
                string mensaje = "";
                try
                {
                    mensaje = $"resultado es : {calc.Calcular()}";
                }
                catch (DivideByZeroException)
                {
                    mensaje = "No se puede dividir por CERO";
                }
                catch (Exception)
                {
                    mensaje = "Error en la operacion";
                }
                ViewData["Resultado"] = mensaje;
            }
            return View();
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View("Error!");
        }
    }
}