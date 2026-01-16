using LocadoraCarro.DAO;
using LocadoraCarro.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace LocadoraCarro.Controllers
{
    public class MarcaController : Controller
    {
        // GET: Marca
        public ActionResult Index()
        {
            var dao = new MarcaDAO();            
            return View(dao.Lista());
        }

        public ActionResult Form()
        {
            return View();
        }
        [HttpPost]
        public ActionResult Adiciona(Marca marca)
        {
            var dao = new MarcaDAO();
            dao.Adiciona(marca);

            return RedirectToAction("Index", "Marca");
        }
        public ActionResult Visualiza(int id)
        {
            var dao = new MarcaDAO();
            ViewBag.Marca = dao.BuscaPorId(id);
            return View();
        }
        public ActionResult Remove(int id)
        {
            var dao = new MarcaDAO();
            dao.Remove(dao.BuscaPorId(id));
            return RedirectToAction("Index");
        }
    }
}