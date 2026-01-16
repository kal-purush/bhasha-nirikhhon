using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using ChajdPizzaWebApp.Data;
using ChajdPizzaWebApp.Models;

namespace ChajdPizzaWebApp.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class OrdersApiController : ControllerBase
    {
        private readonly ApplicationDbContext _context;

        public OrdersApiController(ApplicationDbContext context)
        {
            _context = context;
        }

        // GET: api/OrdersApi
        [HttpGet]
        public async Task<ActionResult<IEnumerable<Order>>> GetOrders()
        {
            return await _context.Orders.ToListAsync();
        }

        // GET: api/OrdersApi/5
        [HttpGet("{id}")]
        public async Task<ActionResult<Order>> GetOrdersModel(int id)
        {
            var ordersModel = await _context.Orders.FindAsync(id);

            if (ordersModel == null)
            {
                return NotFound();
            }

            return ordersModel;
        }

        // PUT: api/OrdersApi/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for
        // more details see https://aka.ms/RazorPagesCRUD.
        [HttpPut("{id}")]
        public async Task<IActionResult> PutOrdersModel(int id, Order ordersModel)
        {
            if (id != ordersModel.Id)
            {
                return BadRequest();
            }

            _context.Entry(ordersModel).State = EntityState.Modified;

            try
            {
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!OrderExists(id))
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

        // POST: api/OrdersApi
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for
        // more details see https://aka.ms/RazorPagesCRUD.
        [HttpPost]
        public async Task<ActionResult<Order>> PostOrdersModel(Order ordersModel)
        {
            _context.Orders.Add(ordersModel);
            await _context.SaveChangesAsync();

            return CreatedAtAction("GetOrdersModel", new { id = ordersModel.Id }, ordersModel);
        }

        // DELETE: api/OrdersApi/5
        [HttpDelete("{id}")]
        public async Task<ActionResult<Order>> DeleteOrdersModel(int id)
        {
            var ordersModel = await _context.Orders.FindAsync(id);
            if (ordersModel == null)
            {
                return NotFound();
            }

            _context.Orders.Remove(ordersModel);
            await _context.SaveChangesAsync();

            return ordersModel;
        }

        private bool OrderExists(int id)
        {
            return _context.Orders.Any(e => e.Id == id);
        }
    }
}