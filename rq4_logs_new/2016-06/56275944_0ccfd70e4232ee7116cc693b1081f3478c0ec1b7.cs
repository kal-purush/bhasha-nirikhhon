using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using SuperERP.Compras;
using SuperERP.Compras.DTO;

namespace SuperERP.Web.Controllers
{
    public class UsuarioController : Controller
    {
        //
        // GET: /Usuario/
        public ActionResult Index()
        {
            return View(Compras.Service.UsuarioService.Listar());
        }

        public ActionResult Incluir()
        {
            return View();
        }

        [HttpPost]
        public ActionResult Incluir(UsuarioDTO usuario)
        {
            Compras.Service.UsuarioService.Cadastrar(usuario);
            return View();
        }

        public ActionResult Alterar(int usuarioID)
        {
            return View(Compras.Service.UsuarioService.BuscaPorId(usuarioID));
        }

        [HttpPost]
        public ActionResult Alterar(UsuarioDTO usuario)
        {
            Compras.Service.UsuarioService.Alterar(usuario);
            return View();
        }

        public ActionResult Excluir(int usuarioID)
        {
            return View(Compras.Service.UsuarioService.BuscaPorId(usuarioID));
        }

        [HttpPost]
        public ActionResult Excluir(UsuarioDTO usuario)
        {
            Compras.Service.UsuarioService.Deletar(usuario.ID);
            return View();
        }

	}
}