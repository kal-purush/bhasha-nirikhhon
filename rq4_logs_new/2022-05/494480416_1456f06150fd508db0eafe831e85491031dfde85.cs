using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using EComerce.Data;
using Shopping.Models;

namespace Shopping.Controllers
{
    public class AvaliacaosController : Controller
    {
        private readonly DBContext _context;

        public AvaliacaosController(DBContext context)
        {
            _context = context;
        }

        // GET: Avaliacaos
        public async Task<IActionResult> Index()
        {
            var dBContext = _context.Avaliacaos.Include(a => a.Client).Include(a => a.Product);
            return View(await dBContext.ToListAsync());
        }

        // GET: Avaliacaos/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null || _context.Avaliacaos == null)
            {
                return NotFound();
            }

            var avaliacao = await _context.Avaliacaos
                .Include(a => a.Client)
                .Include(a => a.Product)
                .FirstOrDefaultAsync(m => m.Id == id);
            if (avaliacao == null)
            {
                return NotFound();
            }

            return View(avaliacao);
        }

        // GET: Avaliacaos/Create
        public IActionResult Create(Compra compra)
        {
            var aval = new Avaliacao();
            aval.ClientCPF = compra.ClientCPF;
            aval.ProductId = compra.ProductId;
            return View(aval);
        }

        // POST: Avaliacaos/Create
        // To protect from overposting attacks, enable the specific properties you want to bind to.
        // For more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("Id,Nota,Comentario,ClientCPF,ProductId")] Avaliacao avaliacao)
        {
            if (ModelState.IsValid)
            {
                _context.Add(avaliacao);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }
            return View(avaliacao);
        }

        // GET: Avaliacaos/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null || _context.Avaliacaos == null)
            {
                return NotFound();
            }

            var avaliacao = await _context.Avaliacaos.FindAsync(id);
            if (avaliacao == null)
            {
                return NotFound();
            }
            return View(avaliacao);
        }

        // POST: Avaliacaos/Edit/5
        // To protect from overposting attacks, enable the specific properties you want to bind to.
        // For more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("Id,Nota,Comentario,ClientCPF,ProductId")] Avaliacao avaliacao)
        {
            if (id != avaliacao.Id)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(avaliacao);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!AvaliacaoExists(avaliacao.Id))
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

            return View(avaliacao);
        }

        // GET: Avaliacaos/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null || _context.Avaliacaos == null)
            {
                return NotFound();
            }

            var avaliacao = await _context.Avaliacaos
                .Include(a => a.Client)
                .Include(a => a.Product)
                .FirstOrDefaultAsync(m => m.Id == id);
            if (avaliacao == null)
            {
                return NotFound();
            }

            return View(avaliacao);
        }

        // POST: Avaliacaos/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            if (_context.Avaliacaos == null)
            {
                return Problem("Entity set 'DBContext.Avaliacaos'  is null.");
            }
            var avaliacao = await _context.Avaliacaos.FindAsync(id);
            if (avaliacao != null)
            {
                _context.Avaliacaos.Remove(avaliacao);
            }
            
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }

        private bool AvaliacaoExists(int id)
        {
          return (_context.Avaliacaos?.Any(e => e.Id == id)).GetValueOrDefault();
        }
    }
}