using API2.Data;
using API2.Entities;
using API2.DTO;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace API2.Controllers
{
    public class BasketController : BaseAPIController
    {
        private readonly StoreContext _context;
        public BasketController(StoreContext context)
        {
            _context = context;
        }
        [HttpGet(Name = "GetBasket")]
        public async Task<ActionResult<BasketDTO>> GetBasket()
        {
            var basket = await RetriveBasket();
            if (basket == null) return NotFound();
            return MapBasketDTO(basket);
        }

        

        [HttpPost(Name = "AddItemtoBasket")]
        public async Task<ActionResult> AddItemtoBasket(int productId,int quantity)
        {
            // get basket if exists 
            var basket = await RetriveBasket();

            //create new if not exists
            if (basket == null) basket=CreateBasket();

            //get product
            Product product = await _context.Products.FindAsync(productId);
            if(product== null) return NotFound();

            //add product to cart
            basket.AddItem(product,quantity);

            var result=await _context.SaveChangesAsync()>0;
            if (result) return CreatedAtRoute("", MapBasketDTO(basket));
            return BadRequest(new ProblemDetails { Title="Error occured while saving data."});

        }

        [HttpDelete(Name = "RemoveItemtoBasket")]
        public async Task<ActionResult> RemoveItemtoBasket(int productId, int quantity)
        {
            //get basket
            var basket=await RetriveBasket();
            if (basket == null) return NotFound();
            Product product = await _context.Products.FindAsync(productId);
            basket.RemoveItem(product, quantity);

            var result = await _context.SaveChangesAsync() > 0;
            if (result) return Ok();
            return BadRequest(new ProblemDetails { Title = "Error occured while saving data." });
        }

        private async Task<Basket> RetriveBasket()
        {
            return await _context.Baskets
                            .Include(i => i.Items)
                            .ThenInclude(x => x.product)
                            .FirstOrDefaultAsync(x => x.BuyerId == Request.Cookies["buyerId"]);
        }
        private Basket CreateBasket()
        {
            var buyerId = Guid.NewGuid().ToString();
            var CookieOptions = new CookieOptions { IsEssential = true, Expires = DateTime.Now.AddDays(20) };
            Response.Cookies.Append("buyerId", buyerId,CookieOptions);
            var basket = new Basket { BuyerId=buyerId};
            _context.Baskets.Add(basket);
            
            return basket;
        }
        private BasketDTO MapBasketDTO(Basket basket)
        {
            return new BasketDTO
            {
                Id = basket.Id,
                buyerId = basket.BuyerId,
                basketItems = basket.Items.Select(item => new BasketItemsDTO
                {
                    Id = item.productId,
                    Name = item.product.Name,
                    pictureUrl = item.product.PictureUrl,
                    Price = item.product.Price,
                    Type = item.product.PictureType,
                    Brand = item.product.Brand,
                    quantity = item.Quantity
                }).ToList()
            };
        }
    }
}