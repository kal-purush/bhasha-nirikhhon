using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using WineShopWebAPI.Authentication;
using WineShopWebAPI.Models;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Identity;
using System.Net;
using Newtonsoft.Json.Linq;

namespace WineShopWebAPI.Controllers
{
    //[Authorize(Roles = "Admin,User")]
    [ApiController]
    [Route("api/[controller]")]
    public class OrdersController : ControllerBase
    {
        private readonly ApplicationDbContext _context;
        private readonly UserManager<User> userManager;
        private readonly RoleManager<IdentityRole> roleManager;
        public OrdersController(UserManager<User> userManager, RoleManager<IdentityRole> roleManager, ApplicationDbContext context)
        {
            this.userManager = userManager;
            this.roleManager = roleManager;
            _context = context;
        }


        // GET: api/Orders
        [HttpGet]
        public async Task<ActionResult<IEnumerable<Order>>> GetOrders()
        {
            return await _context.Orders.ToListAsync();
        }

        // GET: api/Orders/5
        [HttpGet("{id}")]
        public async Task<ActionResult<Order>> GetOrder(int id)
        {
            var order = await _context.Orders.FindAsync(id);

            if (order == null)
            {
                return NotFound();
            }

            return order;
        }

        // POST: api/Orders
        [HttpPost]
      

        public async Task<ActionResult<Order>> PostOrder(ShopOrders shoporder)
        {
            try
            {
                Order order = new Order();
                order.Product_ID = shoporder.Product_ID;
                order.Shop_ID = shoporder.Shop_ID;
                order.Quantity = shoporder.Quantity;
                order.Order_Date = shoporder.Order_Date;
                order.Total_Amount = shoporder.Total_Amount;

                // Check if Shop_ID and Product_ID exist in the respective tables
                var shopExists = await _context.Shops.FindAsync(order.Shop_ID);
                var productExists = await _context.Products.FindAsync(order.Product_ID);

                if (shopExists == null || productExists == null)
                {
                    return BadRequest("Shop or Product not found");
                }

                order.Shop = shopExists;
                order.Product = productExists;

                _context.Orders.Add(order);

                // Update inventory quantity
                var inventoryItem = await _context.Inventories.FirstOrDefaultAsync(i => i.Product_ID == order.Product_ID && i.Shop_ID == order.Shop_ID);
                if (inventoryItem != null)
                {
                    inventoryItem.Quantity -= order.Quantity;
                    _context.Entry(inventoryItem).State = EntityState.Modified;
                }
                else
                {
                    // If inventory item doesn't exist, handle accordingly (e.g., log error)
                }

                await _context.SaveChangesAsync();

                return CreatedAtAction("GetOrder", new { id = order.Order_ID }, order);
            }
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, $"An error occurred: {ex.Message}");
            }
        }



        // PUT: api/Orders/5
        [HttpPut("{id}")]
        public async Task<IActionResult> PutOrder(int id, ShopOrders shoporder)
        {
            try
            {
                Order order = await _context.Orders.FindAsync(id);
                if (order == null)
                {
                    return NotFound();
                }

                order.Product_ID = shoporder.Product_ID;
                order.Shop_ID = shoporder.Shop_ID;
                order.Quantity = shoporder.Quantity;
                order.Order_Date = shoporder.Order_Date;
                order.Total_Amount = shoporder.Total_Amount;

                var shopExists = await _context.Shops.FindAsync(order.Shop_ID);
                var productExists = await _context.Products.FindAsync(order.Product_ID);

                if (shopExists == null || productExists == null)
                {
                    return BadRequest("Shop or Product not found");
                }

                order.Shop = shopExists;
                order.Product = productExists;
                _context.Orders.Update(order);
                // Update inventory quantity
                var inventoryItem = await _context.Inventories.FirstOrDefaultAsync(i => i.Product_ID == order.Product_ID && i.Shop_ID == order.Shop_ID);
                if (inventoryItem != null)
                {
                    inventoryItem.Quantity -= order.Quantity;
                    _context.Entry(inventoryItem).State = EntityState.Modified;
                }
                else
                {
                    // If inventory item doesn't exist, handle accordingly (e.g., log error)
                }

                _context.Entry(order).State = EntityState.Modified;

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
            catch (Exception ex)
            {
                return StatusCode(StatusCodes.Status500InternalServerError, $"An error occurred: {ex.Message}");
            }
        }


        // DELETE: api/Orders/5
        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteOrder(int id)
        {
            var order = await _context.Orders.FindAsync(id);
            if (order == null)
            {
                return NotFound();
            }

            _context.Orders.Remove(order);
            await _context.SaveChangesAsync();

            return NoContent();
        }

        private bool OrderExists(int id)
        {
            return _context.Orders.Any(e => e.Order_ID == id);
        }




    }
}