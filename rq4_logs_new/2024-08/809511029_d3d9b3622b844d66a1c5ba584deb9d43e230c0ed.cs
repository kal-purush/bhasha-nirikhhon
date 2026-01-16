using Application.Commands;
using Application.Queries;
using MediatR;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers
{
    [Route("api/v1/[controller]")]
    [ApiController]
    public class PaymentsController(IMediator _mediator) : ControllerBase
    {
        [HttpGet("{reference}")]
        public async Task<IActionResult> GetPayment(string reference)
        {
            var query = new GetPaymentByReference.Query(reference);
            var invoice = await _mediator.Send(query);
            return Ok(invoice);
        }

        [HttpPost]
        public async Task<IActionResult> MakePayment([FromBody] string reference)
        {
            var command = new InitatePayment.Command { Reference = reference };
            var response = await _mediator.Send(command);
            return Ok(response);
        }

        [HttpGet("callback/{invoiceRef}")]
        public async Task<IActionResult> Callback(string trxref, string invoiceRef)
        {
            var query = new PaymentCallback.Query(trxref, invoiceRef);
            var response = await _mediator.Send(query);
            return Ok(response);
        }
    }
}