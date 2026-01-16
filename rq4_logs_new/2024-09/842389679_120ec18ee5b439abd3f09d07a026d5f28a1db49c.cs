
using Application.Paystack.Commands;
using MediatR;
using Microsoft.AspNetCore.Mvc;



namespace API.Controllers;


[ApiController]
[Route("api/[controller]")]
public class PaymentController : ControllerBase
{
    private readonly IMediator _mediator;

    public PaymentController(IMediator mediator)
    {
        _mediator = mediator;
    }

    [HttpPost("create")]
    public async Task<IActionResult> CreatePayment(CreatePaymentCommand command)
    {
        var result = await _mediator.Send(command);
    

        return Ok(new { paymentUrl = result });
    }

    [HttpPost("verify")]
    public async Task<IActionResult> VerifyPayment(VerifyPaymentCommand command)
    {
        var result = await _mediator.Send(command);
   

        return Ok(result);
    }
}