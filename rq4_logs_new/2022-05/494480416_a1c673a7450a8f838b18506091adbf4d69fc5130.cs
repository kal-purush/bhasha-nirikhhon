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
    public class EstoquesController : Controller
    {
        private readonly DBContext _context;

        public EstoquesController(DBContext context)
        {
            _context = context;
        }

        // GET: Estoques
        public async Task<IActionResult> Index()
        {
            var dBContext = _context.Estoque.Include(e => e.Loja).Include(e => e.Product);
            return View(await dBContext.ToListAsync());
        }

        // GET: Estoques/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null || _context.Estoque == null)
            {
                return NotFound();
            }

            var estoque = await _context.Estoque
                .Include(e => e.Loja)
                .Include(e => e.Product)
                .FirstOrDefaultAsync(m => m.ProductId == id);
            if (estoque == null)
            {
                return NotFound();
            }

            return View(estoque);
        }

        // GET: Estoques/Create
        public IActionResult Create()
        {
            ViewData["LojaId"] = new SelectList(_context.Lojas, "Id", "Nome");
            ViewData["ProductId"] = new SelectList(_context.Products, "Id", "Nome");
            return View();
        }

        // POST: Estoques/Create
        // To protect from overposting attacks, enable the specific properties you want to bind to.
        // For more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("LojaId,ProductId")] Estoque estoque)
        {
            if (ModelState.IsValid)
            {
                _context.Add(estoque);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }
            ViewData["LojaId"] = new SelectList(_context.Lojas, "Id", "Nome", estoque.LojaId);
            ViewData["ProductId"] = new SelectList(_context.Products, "Id", "Nome", estoque.ProductId);
            return View(estoque);
        }

        // GET: Estoques/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null || _context.Estoque == null)
            {
                return NotFound();
            }

            var estoque = await _context.Estoque.FindAsync(id);
            if (estoque == null)
            {
                return NotFound();
            }
            ViewData["LojaId"] = new SelectList(_context.Lojas, "Id", "Nome", estoque.LojaId);
            ViewData["ProductId"] = new SelectList(_context.Products, "Id", "Nome", estoque.ProductId);
            return View(estoque);
        }

        // POST: Estoques/Edit/5
        // To protect from overposting attacks, enable the specific properties you want to bind to.
        // For more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("LojaId,ProductId")] Estoque estoque)
        {
            if (id != estoque.ProductId)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(estoque);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!EstoqueExists(estoque.ProductId))
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
            ViewData["LojaId"] = new SelectList(_context.Lojas, "Id", "Nome", estoque.LojaId);
            ViewData["ProductId"] = new SelectList(_context.Products, "Id", "Nome", estoque.ProductId);
            return View(estoque);
        }

        // GET: Estoques/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null || _context.Estoque == null)
            {
                return NotFound();
            }

            var estoque = await _context.Estoque
                .Include(e => e.Loja)
                .Include(e => e.Product)
                .FirstOrDefaultAsync(m => m.ProductId == id);
            if (estoque == null)
            {
                return NotFound();
            }

            return View(estoque);
        }

        // POST: Estoques/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            if (_context.Estoque == null)
            {
                return Problem("Entity set 'DBContext.Estoque'  is null.");
            }
            var estoque = await _context.Estoque.FindAsync(id);
            if (estoque != null)
            {
                _context.Estoque.Remove(estoque);
            }
            
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }

        private bool EstoqueExists(int id)
        {
          return (_context.Estoque?.Any(e => e.ProductId == id)).GetValueOrDefault();
        }
    }
}