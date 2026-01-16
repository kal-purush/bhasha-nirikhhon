using Microsoft.AspNetCore.Mvc;
using toiec_web.Models;
using toiec_web.Services;
using toiec_web.Services.IService;
using toiec_web.ViewModels.Payment;

namespace toiec_web.Controllers
{
    public class PaymentController : BaseAPIController
    {
        private readonly IPaymentService _paymentService;
        private readonly IPaymentMethodService _paymentMethodService;

        public PaymentController(IPaymentService paymentService, IPaymentMethodService paymentMethodService)
        {
            _paymentService = paymentService;
            _paymentMethodService = paymentMethodService;
        }
        [HttpPost]
        [Route("CreateMoMoPayment")]
        public async Task<IActionResult> CreateMoMoPayment(PaymentInfoModel model)
        {
            var response = await _paymentService.CreatePaymentAsync(model);
            return Ok(response.PayUrl);
        }

        [HttpGet]
        [Route("MomoPaymentCallBack")]
        public async Task<IActionResult> MomoPaymentCallBack()
        {
            Guid paymentMethodId = Guid.Empty;
            var listData = await _paymentMethodService.GetAllPaymentMethods();
            if(listData != null)
            {
                foreach (var paymentMethod in listData)
                {
                    if (paymentMethod.name.Equals("MOMO"))
                    {
                        paymentMethodId = paymentMethod.idMethod;
                        Console.WriteLine(paymentMethod.idMethod);
                    }
                }
                if (paymentMethodId.Equals(Guid.Empty))
                {
                    paymentMethodId = new Guid();
                    var method = new PaymentMethodModel
                    {
                        idMethod = paymentMethodId,
                        name = "MOMO"
                    };
                    await _paymentMethodService.AddPaymentMethod(method);
                }
            }    
            else
            {
                paymentMethodId = new Guid();
                var method = new PaymentMethodModel
                {
                    idMethod = paymentMethodId,
                    name = "MOMO"
                };
                await _paymentMethodService.AddPaymentMethod(method);
            }
            
            var response = _paymentService.MoMoPaymentExecuteAsync(HttpContext.Request.Query, paymentMethodId);
            return Ok(response);
        }
    }
}