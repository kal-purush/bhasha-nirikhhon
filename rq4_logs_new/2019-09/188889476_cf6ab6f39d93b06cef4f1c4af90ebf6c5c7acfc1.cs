using System;
using System.Collections.Generic;
using System.Linq;
using Model;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Services;

namespace collectorShop.Controllers
{
    public class NegociacaoController : Controller
    {
        private readonly IGerenciadorUsuario _gerenciadorUsuario;
        private readonly IServiceNegociacao _serviceNegociacao;

        public NegociacaoController(IGerenciadorUsuario gerenciadorUsuario, IServiceNegociacao serviceNegociacao)
        {
            _gerenciadorUsuario = gerenciadorUsuario;
            _serviceNegociacao = serviceNegociacao;
        }
        // GET: Negociacao
        public ActionResult Index()
        {
            return View(_serviceNegociacao.ObterTodos());
        }

        // GET: Negociacao/Details/5
        public IActionResult Details(int id)
        {
            Negociacao negociacao = _serviceNegociacao.Obter(id);
            return View(negociacao);

        }

        // GET: Negociacao/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: Negociacao/Create
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Create(Negociacao negociacao)
        {
            if (ModelState.IsValid)
            {
                _serviceNegociacao.Inserir(negociacao);
                return RedirectToAction(nameof(Index));
            }
            return View(negociacao);
        }

        // GET: Negociacao/Edit/5
        public ActionResult Edit(int id, Negociacao negociacao)
        {
            if (ModelState.IsValid)
            {
                _serviceNegociacao.Editar(negociacao);
                return RedirectToAction(nameof(Index));
            }
            return View(negociacao);
        }

        // POST: Negociacao/Edit/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Edit(int id, IFormCollection collection)
        {
            Negociacao negociacao = _serviceNegociacao.Obter(id);
            IEnumerable<Usuario> listaUsuario = _gerenciadorUsuario.ObterTodos();
            ViewBag.CodUsuario = new Microsoft.AspNetCore.Mvc.Rendering.SelectList(listaUsuario, "nomeUsuario","codUsuario",
                listaUsuario.Where(usuario => usuario.Nome == negociacao.nomeUsuario).FirstOrDefault());




            return View(negociacao);
            
        }

        // GET: Negociacao/Delete/5
        public ActionResult Delete(int id)
        {
            Negociacao negociacao = _serviceNegociacao.Obter(id);
            return View(negociacao);
        }

        // POST: Negociacao/Delete/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Delete(int id, IFormCollection collection)
        {
            _serviceNegociacao.Remover(id);

            try
            {
                // TODO: Add delete logic here

                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return View();
            }
        }
    }
}