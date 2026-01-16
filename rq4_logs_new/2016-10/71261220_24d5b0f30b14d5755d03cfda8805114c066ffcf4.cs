using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Bangazon.Data;
using Bangazon.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.AspNetCore.Http;

namespace BangazonAPI.Controllers
{
    [ProducesAttribute("application/json")]
    [Route("[controller]")]
    public class CustomersController : Controller
    {


        private BangazonContext context;
        public CustomersController(BangazonContext ctx)
        {
            context = ctx;
        }
        // GET api/values
        [HttpGet]
        public IActionResult Get()
        {
            IQueryable<object> customers = from customer in context.Customer select customer;

            if (customers == null)
            {
                return NotFound();
            }

            return Ok(customers);

        }

        // GET api/values/5
        [HttpGet("{id}", Name = "GetCustomer")]
        public IActionResult Get([FromRoute] int id)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            try
            {
                Customer customer = context.Customer.Single(m => m.CustomerId == id);

                if (customer == null)
                {
                    return NotFound();
                }
                
                return Ok(customer);
            }
            catch (System.InvalidOperationException ex)
            {
                return NotFound();
            }


        }
        // POST method
        public IActionResult Post([FromBody] Customer customer)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            context.Customer.Add(customer);
            try
            {
                context.SaveChanges();
            }
            catch (DbUpdateException)
            {
                if (CustomerExists(customer.CustomerId))
                {
                    return new StatusCodeResult(StatusCodes.Status409Conflict);
                }
                else
                {
                    throw;
                }
            }

            return CreatedAtRoute("GetCustomer", new { id = customer.CustomerId }, customer);
        }

        // PUT api/values/5
        [HttpPut("{id}")]
        public IActionResult Put([FromRoute]int id, [FromBody]Customer customer)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }
            if(customer.CustomerId != id)
            {
                return BadRequest(customer);
            }
            try
            {
                context.Customer.Update(customer);
                context.SaveChanges();
            }
            catch (DbUpdateException)
            {
                return new StatusCodeResult(StatusCodes.Status409Conflict);
            }
            return Ok(customer);
        }

        // DELETE api/values/5
        [HttpDelete("{id}")]
        public IActionResult Delete([FromRoute]int id)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            Customer customer = context.Customer.Single(m => m.CustomerId == id);

            if (customer == null)
            {
                return NotFound();
            }
            else
            {
                context.Customer.Remove(customer);
                    try
                    {
                        context.SaveChanges();
                    }
                    catch (DbUpdateException)
                    {
                        if (CustomerExists(customer.CustomerId))
                        {
                            return new StatusCodeResult(StatusCodes.Status403Forbidden);
                        }
                        else
                        {
                            throw;
                        }
                    }
            }
            return Ok(customer);
        }

        private bool CustomerExists(int id)
        {
            return context.Customer.Count(e => e.CustomerId == id) > 0;
        }
    }
}

