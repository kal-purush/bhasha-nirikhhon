using MediatR;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Ordering.Application.Features.Commands.CheckoutOrder;
using Ordering.Application.Features.Commands.DeleteOrder;
using Ordering.Application.Features.Commands.UpdateOrder;
using Ordering.Application.Features.Queries.Order;
using Ordering.Infrastructure.CQRSBus.Interfaces;

namespace Ordering.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class OrderController : ControllerBase
    {
        private readonly ICommandBus _commandBus;
        private readonly IQueryBus _queryBus;

        public OrderController(ICommandBus commandBus, IQueryBus queryBus)
        {
            _commandBus = commandBus;
            _queryBus = queryBus;
        }

        [HttpGet]
        public async Task<IActionResult> GetOrderByUserName([FromQuery] string userName)
        {
            var query = new GetOrderListQuery(userName);
            var result = await _queryBus.SendAsync(query);
            return Ok(result);
        }

        [HttpPost]
        public async Task<IActionResult> CheckoutOrder([FromBody] CheckoutOrderCommand command)
        {
            var result = await _commandBus.SendAsync(command);
            return Ok(result);
        }

        [HttpPut]
        public async Task<IActionResult> UpdateOrder([FromBody] UpdateOrderCommand command)
        {
            var result = await _commandBus.SendAsync(command);
            return Ok(result);
        }

        [HttpDelete]
        public async Task<IActionResult> DeleteOrder([FromQuery] Guid id)
        {
            var command = new DeleteOrderCommand { Id = id };
            var result = await _commandBus.SendAsync(command);
            return Ok(result);
        }
    }
}