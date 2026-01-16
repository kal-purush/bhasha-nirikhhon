using API.Entities;
using API.Repositories.interfaces;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class BasketController : ControllerBase
    {
        private readonly IBasketRepository _basketRepository;
        public BasketController(IBasketRepository basketRepository)
        {
            _basketRepository = basketRepository;
        }

        [HttpGet]
        [Route("{username}")]
        public async Task<ActionResult<ShoppingCart>> GetBasket([FromRoute] string username)
        {
            var basket = await _basketRepository.GetBasket(username);
            return Ok(basket ?? new ShoppingCart(username));
        }

        [HttpPut]
        public async Task<ActionResult<ShoppingCart>> UpdateBasket([FromBody] ShoppingCart basket)
        {
            var result = await _basketRepository.UpdateBasket(basket);
            return Ok(result);
        }

        [HttpDelete]
        [Route("{username}")]
        public async Task<ActionResult> DeleteBasket([FromRoute] string username)
        {
            await _basketRepository.DeleteBasket(username);
            return Ok();
        } 
    }
}