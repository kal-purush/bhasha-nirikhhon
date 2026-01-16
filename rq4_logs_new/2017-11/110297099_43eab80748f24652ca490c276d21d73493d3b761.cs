using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using CEPiK.Data;
using CEPiK.Models;

namespace CEPiK.Controllers
{
    public class VehicleControlStationsController : Controller
    {
        private readonly CepikContext _context;

        public VehicleControlStationsController(CepikContext context)
        {
            _context = context;
        }

        // GET: VehicleControlStations
        public async Task<IActionResult> Index()
        {
            var cepikContext = _context.VehicleControlStations.Include(v => v.Address).Include(v => v.Entrepreneur);
            return View(await cepikContext.ToListAsync());
        }

        // GET: VehicleControlStations/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var vehicleControlStation = await _context.VehicleControlStations
                .Include(v => v.Address)
                .Include(v => v.Entrepreneur)
                .SingleOrDefaultAsync(m => m.VehicleControlStationID == id);
            if (vehicleControlStation == null)
            {
                return NotFound();
            }

            return View(vehicleControlStation);
        }

        // GET: VehicleControlStations/Create
        public IActionResult Create()
        {
            ViewData["AddressID"] = new SelectList(_context.Addresses, "AddressID", "AddressID");
            ViewData["NIP"] = new SelectList(_context.Entrepreneurs, "NIP", "NIP");
            return View();
        }

        // POST: VehicleControlStations/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("VehicleControlStationID,Name,NIP,AddressID")] VehicleControlStation vehicleControlStation)
        {
            if (ModelState.IsValid)
            {
                _context.Add(vehicleControlStation);
                await _context.SaveChangesAsync();
                return RedirectToAction(nameof(Index));
            }
            ViewData["AddressID"] = new SelectList(_context.Addresses, "AddressID", "AddressID", vehicleControlStation.AddressID);
            ViewData["NIP"] = new SelectList(_context.Entrepreneurs, "NIP", "NIP", vehicleControlStation.NIP);
            return View(vehicleControlStation);
        }

        // GET: VehicleControlStations/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var vehicleControlStation = await _context.VehicleControlStations.SingleOrDefaultAsync(m => m.VehicleControlStationID == id);
            if (vehicleControlStation == null)
            {
                return NotFound();
            }
            ViewData["AddressID"] = new SelectList(_context.Addresses, "AddressID", "AddressID", vehicleControlStation.AddressID);
            ViewData["NIP"] = new SelectList(_context.Entrepreneurs, "NIP", "NIP", vehicleControlStation.NIP);
            return View(vehicleControlStation);
        }

        // POST: VehicleControlStations/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("VehicleControlStationID,Name,NIP,AddressID")] VehicleControlStation vehicleControlStation)
        {
            if (id != vehicleControlStation.VehicleControlStationID)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    _context.Update(vehicleControlStation);
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!VehicleControlStationExists(vehicleControlStation.VehicleControlStationID))
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
            ViewData["AddressID"] = new SelectList(_context.Addresses, "AddressID", "AddressID", vehicleControlStation.AddressID);
            ViewData["NIP"] = new SelectList(_context.Entrepreneurs, "NIP", "NIP", vehicleControlStation.NIP);
            return View(vehicleControlStation);
        }

        // GET: VehicleControlStations/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var vehicleControlStation = await _context.VehicleControlStations
                .Include(v => v.Address)
                .Include(v => v.Entrepreneur)
                .SingleOrDefaultAsync(m => m.VehicleControlStationID == id);
            if (vehicleControlStation == null)
            {
                return NotFound();
            }

            return View(vehicleControlStation);
        }

        // POST: VehicleControlStations/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            var vehicleControlStation = await _context.VehicleControlStations.SingleOrDefaultAsync(m => m.VehicleControlStationID == id);
            _context.VehicleControlStations.Remove(vehicleControlStation);
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }

        private bool VehicleControlStationExists(int id)
        {
            return _context.VehicleControlStations.Any(e => e.VehicleControlStationID == id);
        }
    }
}