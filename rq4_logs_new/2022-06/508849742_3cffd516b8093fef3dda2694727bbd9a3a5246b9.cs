using Checkout.Payment.Api.Models;
using Checkout.Payment.Api.Services;
using Checkout.Payment.Api.Services.Models;
using Microsoft.AspNetCore.Mvc;

namespace Checkout.Payment.Api.Controllers;

[ApiController]
[Route("[controller]")]
public class PaymentController : ControllerBase
{
    private readonly IPaymentService _paymentService;

    public PaymentController(IPaymentService paymentService)
    {
        _paymentService = paymentService;
    }

    [HttpPost]
    public async Task<PaymentResponse> ProcessPayment(ProcessPaymentRequest paymentRequest)
    {
        var result = await _paymentService.ProcessPayment(paymentRequest);
        return result;
    }
}