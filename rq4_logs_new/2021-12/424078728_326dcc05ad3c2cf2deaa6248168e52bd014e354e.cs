using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using Gestao_de_Projetos.Data;
using Gestao_de_Projetos.Models;

namespace Gestao_de_Projetos.Controllers
{
    public class Estado_ProjetoController : Controller
    {
        private readonly Gestao_de_ProjetosContext _context;

        public Estado_ProjetoController(Gestao_de_ProjetosContext context)
        {
            _context = context;
        }

        // GET: Estado_Projeto
        public async Task<IActionResult> Index()
        {
            return View(await _context.Estado_Projeto.ToListAsync());
        }

        // GET: Estado_Projeto/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var estado_Projeto = await _context.Estado_Projeto
                .FirstOrDefaultAsync(m => m.ID_Estado == id);
            if (estado_Projeto == null)
            {
                return NotFound();
            }

            return View(estado_Projeto);
        }

        // GET: Estado_Projeto/Create
        public IActionResult Create()
        {
            return View();
        }

        // POST: Estado_Projeto/Create
        // To protect from overposting attacks, enable the specific properties you want to bind to, for 
        // more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("ID_Estado,Nome")] Estado_Projeto estado_Projeto)
        {
            if (ModelState.IsValid)
            {
                _context.Add(estado_Projeto);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }
            return View(estado_Projeto);
        }

        // GET: Estado_Projeto/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var estado_Projeto = await _context.Estado_Projeto.FindAsync(id);
            if (estado_Projeto == null)
            {
                return NotFound();
            }
            return View(estado_Projeto);
        }

        // POST: Estado_Projeto/Edit/5
        // To protect from overposting attacks, enable the specific properties you want to bind to, for 
        // more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("ID_Estado,Nome")] Estado_Projeto estado_Projeto)
        {
            if (id != estado_Projeto.ID_Estado)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(estado_Projeto);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!Estado_ProjetoExists(estado_Projeto.ID_Estado))
                    {
                        return NotFound();
                    }
                    else
                    {
                        throw;
                    }
                }
                return RedirectToAction(nameof(Index));
            }
            return View(estado_Projeto);
        }

        // GET: Estado_Projeto/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var estado_Projeto = await _context.Estado_Projeto
                .FirstOrDefaultAsync(m => m.ID_Estado == id);
            if (estado_Projeto == null)
            {
                return NotFound();
            }

            return View(estado_Projeto);
        }

        // POST: Estado_Projeto/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            var estado_Projeto = await _context.Estado_Projeto.FindAsync(id);
            _context.Estado_Projeto.Remove(estado_Projeto);
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }

        private bool Estado_ProjetoExists(int id)
        {
            return _context.Estado_Projeto.Any(e => e.ID_Estado == id);
        }
    }
}