using Microsoft.AspNetCore.Mvc;
using OnlineLearningWebAPI.DTOs.request.PaymentRequest;
using OnlineLearningWebAPI.Service.IService;

namespace OnlineLearningWebAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PaymentController : ControllerBase
    {
        private readonly IPaymentService _paymentService;

        public PaymentController(IPaymentService paymentService)
        {
            _paymentService = paymentService;
        }

        [HttpPost("payment-link")]
        public async Task<IActionResult> CreatePaymentLink([FromBody] CreatePaymentLinkRequest request)
        {
            try
            {
                // Chờ đợi hoàn thành trước khi trả về
                var paymentLink = await _paymentService.CreatePaymentLink(request);
                return Ok(new { PaymentLink = paymentLink });
            }
            catch (Exception ex)
            {
                return BadRequest(new { Message = ex.Message });
            }
        }
    }
}