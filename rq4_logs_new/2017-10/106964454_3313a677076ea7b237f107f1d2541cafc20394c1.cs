using LocadoraCarro.DAO;
using LocadoraCarro.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace LocadoraCarro.Controllers
{
    public class ClasseController : Controller
    {
        // GET: Classe
        public ActionResult Index()
        {
            var dao = new ClasseDAO();
            return View(dao.Lista());
        }

        public ActionResult Adiciona(Classe classe)
        {
            var dao = new ClasseDAO();
            dao.Adiciona(classe);
            return RedirectToAction("Index");
        }

        public ActionResult Form()
        {
            return View();
        }

        public ActionResult Visualiza(int id)
        {
            var dao = new ClasseDAO();
            return View(dao.BuscaPorId(id));
        }

        public ActionResult Remove(int id)
        {
            var dao = new ClasseDAO();
            dao.Remove(dao.BuscaPorId(id));
            return RedirectToAction("Index");
        }
    }
}