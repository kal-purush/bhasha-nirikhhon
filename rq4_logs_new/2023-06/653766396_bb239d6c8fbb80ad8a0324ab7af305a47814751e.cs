using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using WebTechnologiesFinalProject.Models;
using WebTechnologiesFinalProject.Requests;

namespace WebTechnologiesFinalProject.Controllers
{
    [Route("api/[controller]")]
    [ApiController]    
    public class CartsController : ControllerBase
    {
        private readonly Context _context;

        public CartsController(Context context)
        {
            _context = context;
        }

        // GET: api/carts
        [HttpGet]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public ActionResult<List<Cart>> Get()
        {
            return _context.Carts.Include(c => c.User).Include(c => c.CartLineItems).ToList();
        }

        // GET api/carts/5
        [HttpGet("{id}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public ActionResult<Cart> Get(int id)
        {
            Cart? cart = _context.Carts.Include(c => c.User)
                                       .Include(c => c.CartLineItems)
                                       .ThenInclude(c => c.Product)
                                       .Where(c => c.CartId == id)
                                       .SingleOrDefault();

            if (cart != null)
            {
                return Ok(cart);
            }
            return NotFound();
        }

        // GET api/carts/user/5
        [HttpGet("user/{id}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public ActionResult<Cart> GetByUserId(int id)
        {
            var cart = _context.Carts.Include(c => c.User).Include(c => c.CartLineItems).FirstOrDefault(c => c.UserId == id);

            if (cart != null)
            {
                return Ok(cart);
            }
            return NotFound();
        }

        // POST api/carts
        [HttpPost]
        [ProducesResponseType(StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public IActionResult Post([FromBody] CartCreateRequest cart)
        {
            try
            {
                var user = _context.Users.Find(cart.UserId);

                if (user == null)
                {
                    return BadRequest("User not found");
                }

                // check if user already has a cart
                var existingCart = _context.Carts.FirstOrDefault(c => c.UserId == cart.UserId);

                if (existingCart != null)
                {
                    return BadRequest("User already has a cart");
                }

                var newCart = new Cart(user);

                _context.Carts.Add(newCart);
                _context.SaveChanges();
                int id = newCart.CartId;
                return CreatedAtAction(nameof(Get), new { id = id }, newCart);
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }

        // PUT api/carts/5
        [HttpPut("{id}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        public IActionResult Put(int id, [FromBody] CartUpdateRequest cart)
        {
            try
            {
                var existingCart = _context.Carts.Include(c => c.User).Include(c => c.CartLineItems).FirstOrDefault(c => c.CartId == id);

                if (existingCart == null)
                {
                    return BadRequest("Cart not found");
                }

                // check for no repeat line items
                var lineItems = cart.CartLineItems;
                var lineItemIds = new List<int>();
                foreach (var lineItem in lineItems)
                {
                    if (lineItemIds.Contains(lineItem.ProductId))
                    {
                        return BadRequest("Cart cannot contain duplicate line items");
                    }
                    lineItemIds.Add(lineItem.ProductId);
                }

                // create the cart line items
                CartLineItem[] cartLineItems = new CartLineItem[cart.CartLineItems.Count];
                for (int i = 0; i < cart.CartLineItems.Count; i++)
                {
                    var cartLineItem = cart.CartLineItems[i];
                    var product = _context.Products.Find(cartLineItem.ProductId);
                    if (product == null)
                    {
                        return BadRequest("Product not found");
                    }
                    cartLineItems[i] = new CartLineItem(existingCart, product, cartLineItem.Quantity);
                }

                existingCart.CartLineItems = cartLineItems.ToList();

                _context.Carts.Update(existingCart);
                _context.SaveChanges();
                return Ok(existingCart);
            }
            catch (Exception ex)
            {
                return BadRequest(ex.Message);
            }
        }

        // DELETE api/carts/5
        [HttpDelete("{id}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public IActionResult Delete(int id)
        {
            var cart = _context.Carts.Find(id);

            if (cart == null)
            {
                return NotFound();
            }

            _context.Carts.Remove(cart);
            _context.SaveChanges();
            return Ok();
        }

        // DELETE api/carts/user/5
        [HttpDelete("user/{id}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public IActionResult DeleteByUserId(int id)
        {
            var cart = _context.Carts.FirstOrDefault(c => c.UserId == id);

            if (cart == null)
            {
                return NotFound();
            }

            _context.Carts.Remove(cart);
            _context.SaveChanges();
            return Ok();
        }
    }
}