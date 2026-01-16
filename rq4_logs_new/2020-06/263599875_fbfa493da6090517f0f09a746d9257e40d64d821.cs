using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using CM.Data;
using CM.Models;

namespace CM.Web.Controllers
{
    public class AddressesController : Controller
    {
        private readonly CMContext _context;

        public AddressesController(CMContext context)
        {
            _context = context;
        }

        //// GET: Addresses
        //public async Task<IActionResult> Index()
        //{
        //    var cMContext = _context.Addresses.Include(a => a.Bar).Include(a => a.City);
        //    return View(await cMContext.ToListAsync());
        //}

        //// GET: Addresses/Details/5
        //public async Task<IActionResult> Details(Guid? id)
        //{
        //    if (id == null)
        //    {
        //        return NotFound();
        //    }

        //    var address = await _context.Addresses
        //        .Include(a => a.Bar)
        //        .Include(a => a.City)
        //        .FirstOrDefaultAsync(m => m.Id == id);
        //    if (address == null)
        //    {
        //        return NotFound();
        //    }

        //    return View(address);
        //}

        // GET: Addresses/Create
        public IActionResult Create()
        {
            ViewData["BarId"] = new SelectList(_context.Bars, "Id", "Name");
            ViewData["CityId"] = new SelectList(_context.Cities, "Id", "Id");
            return View();
        }

        // POST: Addresses/Create
        // To protect from overposting attacks, enable the specific properties you want to bind to, for 
        // more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("Id,CityId,Street,BarId")] Address address)
        {
            if (ModelState.IsValid)
            {
                address.Id = Guid.NewGuid();
                _context.Add(address);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }
            ViewData["BarId"] = new SelectList(_context.Bars, "Id", "Name", address.BarId);
            ViewData["CityId"] = new SelectList(_context.Cities, "Id", "Id", address.CityId);
            return View(address);
        }

        // GET: Addresses/Edit/5
        public async Task<IActionResult> Edit(Guid? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var address = await _context.Addresses.FindAsync(id);
            if (address == null)
            {
                return NotFound();
            }
            ViewData["BarId"] = new SelectList(_context.Bars, "Id", "Name", address.BarId);
            ViewData["CityId"] = new SelectList(_context.Cities, "Id", "Id", address.CityId);
            return View(address);
        }

        // POST: Addresses/Edit/5
        // To protect from overposting attacks, enable the specific properties you want to bind to, for 
        // more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(Guid id, [Bind("Id,CityId,Street,BarId")] Address address)
        {
            if (id != address.Id)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(address);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!AddressExists(address.Id))
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
            ViewData["BarId"] = new SelectList(_context.Bars, "Id", "Name", address.BarId);
            ViewData["CityId"] = new SelectList(_context.Cities, "Id", "Id", address.CityId);
            return View(address);
        }

        //// GET: Addresses/Delete/5
        //public async Task<IActionResult> Delete(Guid? id)
        //{
        //    if (id == null)
        //    {
        //        return NotFound();
        //    }

        //    var address = await _context.Addresses
        //        .Include(a => a.Bar)
        //        .Include(a => a.City)
        //        .FirstOrDefaultAsync(m => m.Id == id);
        //    if (address == null)
        //    {
        //        return NotFound();
        //    }

        //    return View(address);
        //}

        //// POST: Addresses/Delete/5
        //[HttpPost, ActionName("Delete")]
        //[ValidateAntiForgeryToken]
        //public async Task<IActionResult> DeleteConfirmed(Guid id)
        //{
        //    var address = await _context.Addresses.FindAsync(id);
        //    _context.Addresses.Remove(address);
        //    await _context.SaveChangesAsync();
        //    return RedirectToAction(nameof(Index));
        //}

        private bool AddressExists(Guid id)
        {
            return _context.Addresses.Any(e => e.Id == id);
        }
    }
}