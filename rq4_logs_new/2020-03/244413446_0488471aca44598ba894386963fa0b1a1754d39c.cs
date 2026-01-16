using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Cafeteros.Models;

namespace Cafeteros.Controllers
{
    [Produces("application/json")]
    [Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]
    [Route("api/Empleado")]
    public class EmpleadoController : Controller
    {
        private readonly ApplicationDbContext _context;

        public EmpleadoController(ApplicationDbContext context)
        {
            _context = context;
        }

        // GET: api/Empleado
        [HttpGet]
        public IEnumerable<Empleado> GetEmpleado()
        {
            return _context.Empleado;
        }

        // GET: api/Empleado/5
        [HttpGet("{id}")]
        public async Task<IActionResult> GetEmpleado([FromRoute] int id, bool incluirDirecciones = false)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            Empleado Empleado;
            Empleado = await _context.Empleado.SingleOrDefaultAsync(m => m.Id == id);
            

            if (Empleado == null)
            {
                return NotFound();
            }

            return Ok(Empleado);
        }



        // PUT: api/Empleado/5
        // [HttpPut("{id}")]
        // public async Task<IActionResult> PutEmpleado([FromRoute] int id, [FromBody] Empleado Empleado)
        // {

        // }

        // POST: api/Empleado
        [HttpPost]
        public async Task<IActionResult> PostEmpleado([FromBody] Empleado Empleado)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            _context.Empleado.Add(Empleado);
            await _context.SaveChangesAsync();

            return CreatedAtAction("GetEmpleado", new { id = Empleado.Id }, Empleado);
        }

        // DELETE: api/Empleado/5
        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteEmpleado([FromRoute] int id)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            var Empleado = await _context.Empleado.SingleOrDefaultAsync(m => m.Id == id);
            if (Empleado == null)
            {
                return NotFound();
            }

            _context.Empleado.Remove(Empleado);
            await _context.SaveChangesAsync();

            return Ok(Empleado);
        }

        private bool EmpleadoExists(int id)
        {
            return _context.Empleado.Any(e => e.Id == id);
        }
    }
}