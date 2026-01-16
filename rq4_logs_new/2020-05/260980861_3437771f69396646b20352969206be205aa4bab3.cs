using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using CompanyContactManagment.Models;

namespace CompanyContactManagment.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CompanyController : ControllerBase
    {
        private readonly CompanyContactContext _context;

        public CompanyController(CompanyContactContext context)
        {
            _context = context;
        }

        // GET: api/Company
        [HttpGet("all")]
        public async Task<ActionResult<IEnumerable<CompanyModel>>> GetCompanies()
        {
            return await _context.Companies.ToListAsync();
        }

        // GET: api/Company/5
        [HttpGet("{id}")]
        public async Task<ActionResult<CompanyModel>> GetCompanyModel(int id)
        {
            var companyModel = await _context.Companies.FindAsync(id);

            if (companyModel == null)
            {
                return NotFound();
            }

            return companyModel;
        }

        // PUT: api/Company/5
        // To protect from overposting attacks, enable the specific properties you want to bind to, for
        // more details, see https://go.microsoft.com/fwlink/?linkid=2123754.
        [HttpPut("{id}")]
        public async Task<IActionResult> PutCompanyModel(int id, CompanyModel companyModel)
        {
            if (id != companyModel.Id)
            {
                return BadRequest();
            }

            _context.Entry(companyModel).State = EntityState.Modified;

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!CompanyModelExists(id))
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

        // POST: api/Company
        // To protect from overposting attacks, enable the specific properties you want to bind to, for
        // more details, see https://go.microsoft.com/fwlink/?linkid=2123754.
        [HttpPost]
        public async Task<ActionResult<CompanyModel>> PostCompanyModel(CompanyModel companyModel)
        {
            _context.Companies.Add(companyModel);
            await _context.SaveChangesAsync();

            return CreatedAtAction("GetCompanyModel", new { id = companyModel.Id }, companyModel);
        }

        // DELETE: api/Company/5
        [HttpDelete("{id}")]
        public async Task<ActionResult<CompanyModel>> DeleteCompanyModel(int id)
        {
            var companyModel = await _context.Companies.FindAsync(id);
            if (companyModel == null)
            {
                return NotFound();
            }

            _context.Companies.Remove(companyModel);
            await _context.SaveChangesAsync();

            return companyModel;
        }

        private bool CompanyModelExists(int id)
        {
            return _context.Companies.Any(e => e.Id == id);
        }
    }
}