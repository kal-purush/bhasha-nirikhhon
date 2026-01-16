using CartService.Application.DTOs;
using CartService.Application.UseCases.Commands.AddItemToCart;
using CartService.Application.UseCases.Queries.GetItemByIdFromCart;
using CartService.Application.UseCases.Queries.GetItemsFromCart;
using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace CartService.API.Controllers
{
    [ApiController]
    [Route("api/cart")]
    public class CartController : ControllerBase
    {
        private readonly IMediator _mediator;

        public CartController(IMediator mediator)
        {
            _mediator = mediator;
        }

        [HttpGet("items")]
        public async Task<IActionResult> GetItemsFromCart([FromQuery] string userId, CancellationToken cancellationToken)
        {
            var items = await _mediator.Send(new GetItemsFromCartQuery(userId));

            return Ok(items);
        }

        [HttpGet("items/{mealId}")]
        public async Task<IActionResult> GetItemByIdFromCart([FromQuery] string userId, Guid mealId, CancellationToken cancellationToken)
        {
            var item = await _mediator.Send(new GetItemByIdFromCartQuery(userId, mealId));

            return Ok(item);
        }

        [HttpPost]
        public async Task<IActionResult> AddItemToCart([FromBody] CartItemDto cartItem, [FromQuery] string userId, CancellationToken cancellationToken)
        {
            await _mediator.Send(new AddItemToCartCommand(userId, cartItem),cancellationToken);

            return NoContent();
        }


    }
}