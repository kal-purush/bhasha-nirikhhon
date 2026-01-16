using System;
using System.Threading.Tasks;
using Freddy.API.Core;
using Freddy.Application.Core.Commands;
using Freddy.Application.Core.Queries;
using Freddy.Application.Orders.Commands;
using Microsoft.AspNetCore.Mvc;

namespace Freddy.API.Controllers
{
    public class OrdersController : ControllerBase
    {
        private readonly IQueryBus _queryBus;
        private readonly ICommandBus _commandBus;
        private readonly IGuidProvider _guidProvider;

        public OrdersController(IQueryBus queryBus, ICommandBus commandBus, IGuidProvider guidProvider)
        {
            _queryBus = queryBus;
            _commandBus = commandBus;
            _guidProvider = guidProvider;
        }
        
        [HttpGet("api/freddy/customers/{customerId}/orders/{orderId}")]
        public ActionResult GetOrder(Guid customerId, Guid orderId)
        {
            return Ok();
        }

        [HttpPost("api/freddy/customers/{customerId}/orders")]
        public async Task<ActionResult> PostOrder(Guid customerId)
        {
            var orderId = _guidProvider.NewGuid();
            await _commandBus.Handle(new CreateOrderCommand(orderId, customerId));
            return CreatedAtAction(nameof(GetOrder), new { customerId, orderId }, null);
        }
    }
}