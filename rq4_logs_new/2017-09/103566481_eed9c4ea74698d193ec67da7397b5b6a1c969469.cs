using Fiap.MasterChef.Model;
using Fiap.MasterChef.Repository.Interface;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Linq;

namespace Fiap.MasterChef.Controllers
{
    public class CategoriaController : Controller
    {
        private readonly ICategoriaRepository repositorio;

        public CategoriaController(ICategoriaRepository repositorio)
        {
            this.repositorio = repositorio;
        }

        // GET: Categoria
        public IActionResult Index()
        {
            return View(repositorio.GetAll());
        }

        // GET: Categoria/Details/5
        public IActionResult Details(int? id)
        {
            if (!id.HasValue)
            {
                return NotFound();
            }

            var categoriaModel = repositorio.GetById(id.Value);
            if (categoriaModel == null)
            {
                return NotFound();
            }

            return View(categoriaModel);
        }

        // GET: Categoria/Create
        public IActionResult Create()
        {
            return View();
        }

        // POST: Categoria/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public IActionResult Create([Bind("Id,Nome")] CategoriaModel categoriaModel)
        {
            if (ModelState.IsValid)
            {
                repositorio.Add(categoriaModel);
                return RedirectToAction("Index");
            }
            return View(categoriaModel);
        }

        // GET: Categoria/Edit/5
        public IActionResult Edit(int? id)
        {
            if (!id.HasValue)
            {
                return NotFound();
            }

            var categoriaModel = repositorio.GetById(id.Value);
            if (categoriaModel == null)
            {
                return NotFound();
            }
            return View(categoriaModel);
        }

        // POST: Categoria/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public IActionResult Edit(int id, [Bind("Id,Nome")] CategoriaModel categoriaModel)
        {
            if (id != categoriaModel.Id)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    repositorio.Update(categoriaModel);
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!CategoriaModelExists(categoriaModel.Id))
                    {
                        return NotFound();
                    }
                    else
                    {
                        throw;
                    }
                }
                return RedirectToAction("Index");
            }
            return View(categoriaModel);
        }

        // GET: Categoria/Delete/5
        public IActionResult Delete(int? id)
        {
            if (!id.HasValue)
            {
                return NotFound();
            }

            var categoriaModel = repositorio.GetById(id.Value);
            if (categoriaModel == null)
            {
                return NotFound();
            }

            return View(categoriaModel);
        }

        // POST: Categoria/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public IActionResult DeleteConfirmed(int id)
        {
            var categoriaModel = repositorio.GetById(id);

            repositorio.Remove(categoriaModel);
            return RedirectToAction("Index");
        }

        private bool CategoriaModelExists(int id)
        {
            return repositorio.GetAll().Any(c => c.Id == id);
        }
    }
}