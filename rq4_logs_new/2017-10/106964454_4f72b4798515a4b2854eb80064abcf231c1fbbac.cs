using LocadoraCarro.DAO;
using LocadoraCarro.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace LocadoraCarro.Controllers
{
    public class ClienteController : Controller
    {
        // GET: Cliente
        public ActionResult Index()
        {
            var dao = new ClienteDAO();
            return View(dao.Lista());
        }

        public ActionResult Form()
        {
            return View();
        }

        public ActionResult Adiciona(Cliente cliente)
        {
            var dao = new ClienteDAO();
            dao.Adiciona(cliente);
            return RedirectToAction("Index");
        }

        public ActionResult Visualiza(int id)
        {
            var dao = new ClienteDAO();
            return View(dao.BuscaPorId(id));
        }

        public ActionResult Remove(int id)
        {
            var dao = new ClienteDAO();
            dao.Remove(dao.BuscaPorId(id));
            return RedirectToAction("Index");
        }
    }
}