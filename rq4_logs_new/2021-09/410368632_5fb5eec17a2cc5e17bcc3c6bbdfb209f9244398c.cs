using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AutenticacionCoordenadas.Controllers
{
    public class AdministradorController : Controller
    {
        public IConfiguration Configuration { get; }

        public AdministradorController(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IActionResult Index()
        {
            return View();
        }

        public IActionResult Actualiza()
        {
            return View();
        }

        public IActionResult Buscar()
        {
            return View();
        }

        public IActionResult Registro()
        {
            return View();
        }

        public IActionResult Solicitud()
        {
            return View();
        }

    }
}