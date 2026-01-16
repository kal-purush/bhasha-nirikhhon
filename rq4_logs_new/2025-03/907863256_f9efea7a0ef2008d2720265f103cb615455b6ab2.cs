using CaptoneProject_IOTS_Service.Services.Interface;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CaptoneProject_IOTS_API.Controllers.OrderItemController
{
    [Route("api/[controller]")]
    [ApiController]
    public class OrderItemController : ControllerBase
    {
        private readonly IOrderItemService _orderItemService;

        public OrderItemController(IOrderItemService orderItemService)
        {
            _orderItemService = orderItemService;
        }

        [HttpGet("get-order-details-by-order-id")]
        public async Task<IActionResult> GetOrderDetailsByOrderId([FromQuery] int orderId)
        {
            var result = await _orderItemService.getOrderDetailsByOrderId(orderId);

            if (!result.IsSuccess)
                return BadRequest(result);

            return Ok(result);
        }

        [HttpGet("get-order-details-by-store-id")]
        public async Task<IActionResult> GetOrderDetailsByStoreId()
        {
            var result = await _orderItemService.getOrderDetailsByStoreId();

            if (!result.IsSuccess)
                return BadRequest(result);

            return Ok(result);
        }
    }
}