//using Microsoft.AspNetCore.Authorization;
//using Microsoft.AspNetCore.Mvc;
//using WineShopWebAPI.Models;
//using Microsoft.Identity.Web.Resource;
//using Microsoft.AspNetCore.Identity;
//using Microsoft.EntityFrameworkCore;
//using WineShopWebAPI.Authentication;

//namespace WineShopWebAPI.Controllers
//{
//    [Authorize(Roles = "StoreManager")]
//    [ApiController]
//    [Route("api/[controller]")]
//    public class StoreManagerController : ControllerBase
//    {
//        private readonly ApplicationDbContext _context;
//        public StoreManagerController(ApplicationDbContext context)
//        {

//            _context = context;
//        }


//        // GET: api/PurchaseManager/PurchaseRegisters
//        [HttpGet("PurchaseRegisters")]
//        public async Task<ActionResult<IEnumerable<PurchaseRegister>>> GetPurchaseRegisters()
//        {
//            return await _context.PurchaseRegisters.Include(p => p.Supplier)
//                                                    .Include(p => p.ChartOfAccount)
//                                                    .Include(p => p.Item)
//                                                    .ToListAsync();
//        }

//        // GET: api/PurchaseManager/PurchaseRegisters/5
//        [HttpGet("PurchaseRegisters/{id}")]
//        public async Task<ActionResult<PurchaseRegister>> GetPurchaseRegister(int id)
//        {
//            var purchaseRegister = await _context.PurchaseRegisters.FindAsync(id);

//            if (purchaseRegister == null)
//            {
//                return NotFound();
//            }

//            return purchaseRegister;
//        }

//        // PUT: api/PurchaseManager/PurchaseRegisters/5
//        [HttpPut("PurchaseRegisters/{id}")]
//        public async Task<IActionResult> PutPurchaseRegister(int id, PurchaseRegister purchaseRegister)
//        {
//            if (id != purchaseRegister.Id)
//            {
//                return BadRequest();
//            }

//            _context.Entry(purchaseRegister).State = EntityState.Modified;

//            try
//            {
//                await _context.SaveChangesAsync();
//            }
//            catch (DbUpdateConcurrencyException)
//            {
//                if (!PurchaseRegisterExists(id))
//                {
//                    return NotFound();
//                }
//                else
//                {
//                    throw;
//                }
//            }

//            return NoContent();
//        }

//        // POST: api/PurchaseManager/PurchaseRegisters
//        [HttpPost("PurchaseRegisters")]
//        public async Task<ActionResult<PurchaseRegister>> PostPurchaseRegister(PurchaseRegister purchaseRegister)
//        {
//            _context.PurchaseRegisters.Add(purchaseRegister);
//            await _context.SaveChangesAsync();

//            return CreatedAtAction(nameof(GetPurchaseRegister), new { id = purchaseRegister.Id }, purchaseRegister);
//        }

//        // DELETE: api/PurchaseManager/PurchaseRegisters/5
//        [HttpDelete("PurchaseRegisters/{id}")]
//        public async Task<IActionResult> DeletePurchaseRegister(int id)
//        {
//            var purchaseRegister = await _context.PurchaseRegisters.FindAsync(id);
//            if (purchaseRegister == null)
//            {
//                return NotFound();
//            }

//            _context.PurchaseRegisters.Remove(purchaseRegister);
//            await _context.SaveChangesAsync();

//            return NoContent();
//        }

//        private bool PurchaseRegisterExists(int id)
//        {
//            return _context.PurchaseRegisters.Any(e => e.Id == id);
//        }



//        // GET: api/PurchaseManager/Products
//        [HttpGet("Products")]
//        public async Task<ActionResult<IEnumerable<Item>>> GetItems()
//        {
//            return await _context.Items.ToListAsync();
//        }

//        // GET: api/PurchaseManager/Products/5
//        [HttpGet("Products/{id}")]
//        public async Task<ActionResult<Item>> GetItem(int id)
//        {
//            var item = await _context.Items.FindAsync(id);

//            if (item == null)
//            {
//                return NotFound();
//            }

//            return item;
//        }

//        // PUT: api/PurchaseManager/Products/5
//        [HttpPut("Products/{id}")]
//        public async Task<IActionResult> PutItem(int id, Item item)
//        {
//            if (id != item.Id)
//            {
//                return BadRequest();
//            }

//            _context.Entry(item).State = EntityState.Modified;

//            try
//            {
//                await _context.SaveChangesAsync();
//            }
//            catch (DbUpdateConcurrencyException)
//            {
//                if (!ItemExists(id))
//                {
//                    return NotFound();
//                }
//                else
//                {
//                    throw;
//                }
//            }

//            return NoContent();
//        }

//        // POST: api/PurchaseManager/Products
//        [HttpPost("Products")]
//        public async Task<ActionResult<Item>> PostItem(Item item)
//        {
//            _context.Items.Add(item);
//            await _context.SaveChangesAsync();

//            return CreatedAtAction(nameof(GetItem), new { id = item.Id }, item);
//        }

//        // DELETE: api/PurchaseManager/Products/5
//        [HttpDelete("Products/{id}")]
//        public async Task<IActionResult> DeleteItem(int id)
//        {
//            var item = await _context.Items.FindAsync(id);
//            if (item == null)
//            {
//                return NotFound();
//            }

//            _context.Items.Remove(item);
//            await _context.SaveChangesAsync();

//            return NoContent();
//        }

//        private bool ItemExists(int id)
//        {
//            return _context.Items.Any(e => e.Id == id);
//        }

//        // GET: api/PurchaseManager/ViewInventory
//        [HttpGet("ViewInventory")]
//        public async Task<ActionResult<IEnumerable<ItemStock>>> GetItemStocks()
//        {
//            return await _context.ItemStocks.Include(i => i.Item).ToListAsync();
//        }

//    }
//}

