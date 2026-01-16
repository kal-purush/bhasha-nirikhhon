using LocadoraCarro.DAO;
using LocadoraCarro.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace LocadoraCarro.Controllers
{
    public class ProtecaoController : Controller
    {
        // GET: Protecao
        public ActionResult Index()
        {
            var dao = new ProtecaoDAO();
            return View(dao.Lista());
        }

        public ActionResult Adiciona(Protecao protecao)
        {
            var dao = new ProtecaoDAO();
            dao.Adiciona(protecao);
            return RedirectToAction("Index");
        }

        public ActionResult Form()
        {
            return View();
        }

        public ActionResult Visualiza(int id)
        {
            var dao = new ProtecaoDAO();
            return View(dao.BuscaPorId(id));
        }

        public ActionResult Remove(int id)
        {
            var dao = new ProtecaoDAO();
            dao.Remove(dao.BuscaPorId(id));
            return RedirectToAction("Index");
        }
    }
}