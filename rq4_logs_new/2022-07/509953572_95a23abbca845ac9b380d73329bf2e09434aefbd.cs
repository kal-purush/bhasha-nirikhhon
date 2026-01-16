using Microsoft.AspNetCore.Mvc;
using DevFreela.Payments.Api.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DevFreela.Payments.Api.Service;
namespace DevFreela.Payments.Api.Controllers
{
  [Route("api/payments")]
  public class PaymentsController : ControllerBase
  {
    public readonly IPaymentService _paymentService;
    public PaymentsController(IPaymentService paymentService)
    {
      _paymentService = paymentService;
    }
    [HttpPost]
    public async Task<IActionResult> Post([FromBody] PaymentInfoInputModel paymentInfoInputModel)
    {
      var result = await _paymentService.Process(paymentInfoInputModel);
      if (!result)
      {
        return BadRequest();
      }
      return NoContent();
    }
  }
}