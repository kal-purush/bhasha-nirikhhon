using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Services;
using Model;
using Persistence;
namespace collectorShop.Controllers
{ }
    /*
    public class EnvioController : Controller
    {
        private readonly IGerenciadorEnvio gerenciadorEnvio;
    private readonly IServiceNegociacao gerenciadorNegociacao;
 //   private readonly ServiceCorreios.CalcPrecoPrazoWSSoap wsCorreios;

    public EnvioController(IGerenciadorEnvio _gerenciadorEnvio, IServiceNegociacao _gerenciadorNegociacao)
      //  ,  ServiceCorreios.CalcPrecoPrazoWSSoap _wsCorreios)
    {
        gerenciadorEnvio = _gerenciadorEnvio;
       
      //  wsCorreios = _wsCorreios;
    }
        
    // GET: Anuncio
    public ActionResult Index()
    {
        return View(gerenciadorAnuncio.ObterTodos());
    }

    // GET: Anuncio/Details/5
    public ActionResult Details(int id)
    {
        Anuncio anuncio = gerenciadorAnuncio.Obter(id);
        return View(anuncio);
    }

    // GET: Anuncio/Create
    public ActionResult Create()
    {
        ViewBag.CodUsuario = new SelectList(gerenciadorUsuario.ObterTodos(), "CodUsuario", "Nome", null);

        return View();
    }

    // POST: Anuncio/Create
    [HttpPost]
    [ValidateAntiForgeryToken]
    public ActionResult Create(Anuncio anuncio)
    {
        if (ModelState.IsValid)
        {
            gerenciadorAnuncio.Inserir(anuncio);
            return RedirectToAction(nameof(Index));
        }
        return View(anuncio);
    }

    // GET: Anuncio/Edit/5
    public ActionResult Edit(int id)
    {
        Anuncio anuncio = gerenciadorAnuncio.Obter(id);
        IEnumerable<Usuario> listaUsuario = gerenciadorUsuario.ObterTodos();
        ViewBag.CodUsuario = new SelectList(listaUsuario, "CodUsuario", "Nome",
            listaUsuario.Where(usuario => usuario.CodUsuario == anuncio.CodUsuario).FirstOrDefault());
        return View(anuncio);

    }

    // POST: Anuncio/Edit/5
    [HttpPost]
    [ValidateAntiForgeryToken]
    public ActionResult Edit(int id, Anuncio anuncio)
    {
        if (ModelState.IsValid)
        {
            gerenciadorAnuncio.Editar(anuncio);
            return RedirectToAction(nameof(Index));
        }
        return View(anuncio);

    }

    // GET: Anuncio/Delete/5
    public ActionResult Delete(int id)
    {
        Anuncio anuncio = gerenciadorAnuncio.Obter(id);
        return View(anuncio);
    }

    // POST: Anuncio/Delete/5
    [HttpPost]
    [ValidateAntiForgeryToken]
    public ActionResult Delete(int id, IFormCollection collection)
    {
        gerenciadorAnuncio.Remover(id);
        return RedirectToAction(nameof(Index));
    }

}
*/