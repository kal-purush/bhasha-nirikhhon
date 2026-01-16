using CaptoneProject_IOTS_Service.Services.Interface;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CaptoneProject_IOTS_API.Controllers.ShippingController
{
    [Route("api/[controller]")]
    [ApiController]
    public class ShippingController : ControllerBase
    {
        private readonly IGHTKService _shippingService;

        public ShippingController(IGHTKService shippingService)
        {
            _shippingService = shippingService;
        }

        [HttpPost("create/{orderId}")]
        public async Task<IActionResult> CreateShipment(int orderId)
        {

            var success = await _shippingService.CreateShipmentAsync(orderId);
            if (success)
            {
                return Ok(new { message = "Đơn hàng đã được gửi tới GHTK thành công." });
            }
            return BadRequest(new { message = "Không thể gửi đơn hàng tới GHTK." });
        }
    }
}