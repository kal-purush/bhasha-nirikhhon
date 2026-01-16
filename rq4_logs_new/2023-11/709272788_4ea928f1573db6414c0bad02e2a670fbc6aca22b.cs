using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.Extensions.Configuration;
using Turnos.Models;
using System.Linq;

namespace Turnos.Controllers
{
    public class TurnoController : Controller
    {
        private readonly TurnosContext _context;

        private readonly IConfiguration _configuration;

        public TurnoController(TurnosContext context, IConfiguration configuration)
        {
            _context = context;
            _configuration = configuration;
        }

        public IActionResult Index()
        {
            ViewData["IdMedico"] = new SelectList((from medico in _context.Medicos
                                                   .ToList() select new {IdMedico = medico.IdMedico, NombreCompleto = medico.Nombre +" "+ medico.Apellido }),"IdMedico", "NombreCompleto");
            ViewData["IdPaciente"] = new SelectList((from paciente in _context.Pacientes
                                                   .ToList()
                                                   select new { IdPaciente = paciente.IdPaciente, NombreCompleto = paciente.Nombre + " " + paciente.Apellido }), "IdPaciente", "NombreCompleto");
            return View();
        }

        public JsonResult ObtetenerTurnos(int idMedico)
        {
            List<Turno> turnos = new List<Turno>();
            turnos = _context.Turnos.Where(t => t.IdMedico == idMedico).ToList();

            return Json(turnos);
        }

        [HttpPost]
        public JsonResult GrabarTurno(Turno turno)
        {
            var ok = false;
            try
            {
                _context.Turnos.Add(turno);
                _context.SaveChanges();
                ok = true;

            }
            catch (Exception e)
            {
                Console.WriteLine("{0} Excepccion encontrada", e);
            }
            var jsonResult = new { ok = ok };
            return Json(jsonResult);
        }

        [HttpPost]
        public JsonResult EliminarTruno(int idTurno)
        {
            var ok = false;
            try
            {
                var turnoAEliminar = _context.Turnos.Where(t => t.IdTurno == idTurno).FirstOrDefault();
                if (turnoAEliminar != null)
                {
                    _context.Turnos.Remove(turnoAEliminar);
                    _context.SaveChanges();
                    ok = true;
                }

            }
            catch (Exception e)
            {
                Console.WriteLine("{0} Excepccion encontrada", e);
            }
            var jsonResult = new { ok = ok };
            return Json(jsonResult);
        }


    }
}