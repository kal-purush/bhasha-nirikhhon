using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Doctors;
using Doctors.Data;
using Microsoft.AspNetCore.Identity;

namespace Doctors.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CompaniesController : ControllerBase
    {

        #region CTOR

        private readonly ApplicationDbContext _context;
        private readonly UserManager<IdentityUser> _userManager;

        public CompaniesController(ApplicationDbContext context,
                UserManager<IdentityUser> userManager
            )
        {
            _context = context;
            _userManager = userManager;

        }

        #endregion


        [HttpGet("get-test")]
        public async Task<JsonResult> GetTset()
        {
            var ss = new Company()
            {
                CompanyName = "kiloo",
                CompanyTitle = "ghanooon sazi"
            };

            return await JsonH.SuccessAsync(ss);
        }

        [HttpGet("get-my-company")]
        public async Task<ActionResult<Company>> GetMyCompany()
        {
            System.Threading.Thread.Sleep(10);
            var userId = HttpContext.Items["userId"];
            var company = await _context.Companies.FirstOrDefaultAsync(c=>c.OwnerId == userId.ToString());

            company.ContactInfos = _context.ContactInfos.Where(c=>c.Company.Id == company.Id).ToList();
            if (company == null)
            {
                return await JsonH.ErrorAsync("Company Not Found.");
            }
            return await JsonH.SuccessAsync(company);
        }



        [HttpGet]
        public async Task<ActionResult<IEnumerable<Company>>> GetCompanies()
        {
            return await _context.Companies.ToListAsync();
        }


        [HttpGet("{id}")]
        public async Task<ActionResult<Company>> GetCompany(string id)
        {
            var company = await _context.Companies.FindAsync(id);

            if (company == null)
            {
                return NotFound();
            }

            return company;
        }



        [HttpPut("{id}")]
        public async Task<IActionResult> PutCompany(string id, Company company)
        {
            if (id != company.Id)
            {
                return BadRequest();
            }

            _context.Entry(company).State = EntityState.Modified;

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!CompanyExists(id))
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



        [HttpPost("new-company")]
        public async Task<ActionResult<Company>> PostCompany(Company company)
        {

            try
            {
                company.Id = Guid.NewGuid().ToString();

                ICollection<ContactInfo> contacts = new List<ContactInfo>();

                foreach (var item in company.ContactInfos)
                {
                    contacts.Add(new ContactInfo()
                    {
                        Company = company,
                        Type = item.Type,
                        Value = item.Value,
                        Id = Guid.NewGuid().ToString()
                    }); ;
                }
                ////_context.ContactInfos.AddRange(contacts);
                //try
                //{
                //    //await _context.SaveChangesAsync();
                //}
                //catch (DbUpdateException)
                //{

                //    return await JsonH.ErrorAsync("Conflict Contact Infos");

                //}
                IdentityUser _user;
                try
                {
                    //var userId = HttpContext.Items["userId"];
                    //if (userId == null)
                    //    return await JsonH.ErrorAsync("Couldn't find the users Id in httpContext.");
                    //_user = await _userManager.FindByIdAsync(userId.ToString());
                    //if (_user == null)
                    //    return await JsonH.ErrorAsync("Couldn't find the user in DB.");
                }
                catch (Exception ex)
                { return await JsonH.ErrorAsync(ex.Message); }

                string newId = Guid.NewGuid().ToString();

                //UsersCompany nUC = new UsersCompany();
                //nUC.CompanyId = company.Id;
                //nUC.UserId = _user.Id;

                //_context.UsersCompany.Add(nUC);
                var userId = HttpContext.Items["userId"];
                company.OwnerId = userId.ToString();
                company.ContactInfos = new List<ContactInfo>(contacts);
                _context.Companies.Add(company);
                try
                {
                    await _context.SaveChangesAsync();
                }
                catch (DbUpdateException ex)
                {
                    if (CompanyExists(company.Id))
                    {
                        return await JsonH.ErrorAsync("Conflict company id");
                        //return Conflict();
                    }
                    else
                    {
                        return await JsonH.ErrorAsync("Db Exception: " + ex.Message);
                    }
                }

                return company;
                //return await JsonH.SuccessAsync(company);

            }
            catch (Exception ex)
            {

                return await JsonH.ErrorAsync(ex.Message);

            }
            //return CreatedAtAction("GetCompany", new { id = company.Id }, company);
        }

        [HttpDelete("{id}")]
        public async Task<ActionResult<Company>> DeleteCompany(string id)
        {
            var company = await _context.Companies.FindAsync(id);
            if (company == null)
            {
                return NotFound();
            }

            _context.Companies.Remove(company);
            await _context.SaveChangesAsync();

            return company;
        }

        private bool CompanyExists(string id)
        {
            return _context.Companies.Any(e => e.Id == id);
        }
    }
}