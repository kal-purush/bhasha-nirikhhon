using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Studio_Inventory_API;
using Studio_Inventory_API.Models;

namespace Studio_Inventory_API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class EquipmentListController : ControllerBase
    {
        private readonly StudioContext _context;

        public EquipmentListController(StudioContext context)
        {
            _context = context;
        }

        // GET: api/EquipmentList
        [HttpGet]
        public async Task<ActionResult<IEnumerable<Equipment>>> GetEquipmentList()
        {
            return await _context.EquipmentList.ToListAsync();
        }

        // GET: api/EquipmentList/5
        [HttpGet("{id}")]
        public async Task<ActionResult<Equipment>> GetEquipment(int id)
        {
            var equipment = await _context.EquipmentList.FindAsync(id);

            if (equipment == null)
            {
                return NotFound();
            }

            return equipment;
        }

        // PUT: api/EquipmentList/5
        // To protect from overposting attacks, enable the specific properties you want to bind to, for
        // more details, see https://go.microsoft.com/fwlink/?linkid=2123754.
        [HttpPut("{id}")]
        public async Task<IActionResult> PutEquipment(int id, Equipment equipment)
        {
            if (id != equipment.Id)
            {
                return BadRequest();
            }

            _context.Entry(equipment).State = EntityState.Modified;

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!EquipmentExists(id))
                {
                    return NotFound();
                }
                else
                {
                    throw;
                }
            }

            return NoContent();
        }

        // POST: api/EquipmentList
        // To protect from overposting attacks, enable the specific properties you want to bind to, for
        // more details, see https://go.microsoft.com/fwlink/?linkid=2123754.
        [HttpPost]
        public async Task<ActionResult<Equipment>> PostEquipment(Equipment equipment)
        {
            _context.EquipmentList.Add(equipment);
            await _context.SaveChangesAsync();

            return CreatedAtAction("GetEquipment", new { id = equipment.Id }, equipment);
        }

        // DELETE: api/EquipmentList/5
        [HttpDelete("{id}")]
        public async Task<ActionResult<Equipment>> DeleteEquipment(int id)
        {
            var equipment = await _context.EquipmentList.FindAsync(id);
            if (equipment == null)
            {
                return NotFound();
            }

            _context.EquipmentList.Remove(equipment);
            await _context.SaveChangesAsync();

            return equipment;
        }

        private bool EquipmentExists(int id)
        {
            return _context.EquipmentList.Any(e => e.Id == id);
        }
    }
}