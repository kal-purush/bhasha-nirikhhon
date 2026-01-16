using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AngularApiApp.Models;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace AngularApiApp.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class EmployeeApiController : ControllerBase
    {
        private DemoDBContext db = new DemoDBContext();
     
        [HttpGet]
        [Route("/api/getcityList")]
     //   [EnableCors("AllowOrigin")]
        public async Task<IActionResult>  GetCity()
        {
            using (DemoDBContext db=new DemoDBContext())
            {
                var data = await db.Cities.Select(x => new 
                {
                    Id = x.Id,
                    CityName = x.CityName

                }).ToListAsync();

                return  Ok (data) ;
            }
            return NotFound();
        }
        [HttpPost]
        [Route("/api/saveEmploye")]
     
        public async Task<IActionResult> SaveEmployee(  Employee model)
        {
            try
            {

           
            using (DemoDBContext db =new DemoDBContext())
            {
                 await db.Employees.AddAsync(model);
                  db.SaveChanges();

                return Ok(model.Id);
            }
            }
            catch (Exception ex)
            {
                var messsage = ex.Message;
                return Ok(model.Id);
                throw;
            }
          

        }
        [HttpGet]
        [Route("/api/getEmployeelist")]
        public async Task<ActionResult>  GetEmployeeelist()
        {
           try
           {

            var data = await db.Employees.Select(x=>new 
            { 
                id=x.Id,
                name=x.Name,
                city=x.City??"",
                department=x.Department??"",
                gender=x.Gender??""
            
            }).ToListAsync(); 
            
            return Ok(data);
           }
           catch (Exception ex)
           {

                var message = ex.Message;
                return Ok(false);
           }
        }
        [HttpPost]
        [Route("/api/deleteEmpById")]
        public async Task<ActionResult> DeleteEmployeById([FromQuery]  int id)
        {
            try
            {
                var data = await db.Employees.Where(x => x.Id == id).FirstOrDefaultAsync();

                db.Employees.Remove(data);
                db.SaveChanges();

                return Ok(data);
            }
            catch (Exception ex)
            {

                var message = ex.Message;
                return NotFound();
            }
        }
        [HttpPost]
        [Route("/api/EditEmp")]
        public async Task<ActionResult> EditEmployee(int id)
        {
            try
            {
                var data = await db.Employees.FindAsync(id);

                db.Employees.Remove(data);
                db.SaveChanges();

                return Ok(data);
            }
            catch (Exception ex)
            {

                var message = ex.Message;
                return Ok(false);
            }
        }

        // PUT: api/EmployeeApi/5
        [HttpPut("{id}")]
        public void Put(int id, [FromBody] string value)
        {
        }

        // DELETE: api/ApiWithActions/5
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
        }
    }
}