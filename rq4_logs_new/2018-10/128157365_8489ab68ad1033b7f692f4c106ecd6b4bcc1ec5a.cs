using Silabus.Models;
using Silabus.Servicios;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace Silabus.Controllers
{
    public class ProgramaFormativoController : Controller
    {
        private SilaboRepositorio _repo;

        public ProgramaFormativoController()
        {
            _repo = new SilaboRepositorio();
        }

        public ActionResult ProgramaFormativo(int id = 1)
        {
            Silabo silabo = _repo.ObtenerSilabo(id);
            return View(silabo);
        }

        public ActionResult CambiarFase(Silabo silaboTem)
        {
            Silabo silabo = _repo.ObtenerSilabo(silaboTem.Id);
            silabo.SelectedSilaboFase = silaboTem.SelectedSilaboFase;
            return View("ProgramaFormativo", silabo);
        }
    }
}