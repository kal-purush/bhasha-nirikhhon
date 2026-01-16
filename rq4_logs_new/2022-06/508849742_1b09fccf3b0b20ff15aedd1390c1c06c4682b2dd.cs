using Bank.Api.Models;
using Bank.Api.Services;
using Microsoft.AspNetCore.Mvc;

namespace Bank.Api.Controllers;

[ApiController]
[Route("[controller]")]
public class PaymentController : ControllerBase
{
    private readonly IPaymentService _paymentService;
    private readonly ILogger<PaymentController> _logger;

    public PaymentController(IPaymentService paymentService, ILogger<PaymentController> logger)
    {
        _paymentService = paymentService;
        _logger = logger;
    }

    [HttpPost]
    public async Task<ProcessPaymentResponse> ProcessPayment(ProcessPaymentRequest paymentRequest)
    {
        var result = await _paymentService.ProcessPayment(paymentRequest);
        return result;
    }
}
