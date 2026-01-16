using Microsoft.AspNetCore.Mvc;
using AgravitaeWebExtension.Helper;
using AgravitaeWebExtension.Repositories;
using AgravitaeWebExtension.Services.ZiplingoEngagementService;
using AgravitaeExtension.Merchants.Tyga.Interfaces;
using AgravitaeExtension.Merchants.Tyga.Models;

namespace AgravitaeWebExtension.Controllers
{
    [Route("api/[controller]")]
    public class MerchantsController : Controller
    {
        private readonly IZiplingoEngagementRepository _ziplingoEngagementRepository;
        private readonly ICustomLogRepository _customLogRepository;
        private readonly ITygaService _tygaService;

        public MerchantsController(
            IZiplingoEngagementRepository ziplingoEngagementRepository, 
            ICustomLogRepository customLogRepository,
            ITygaService tygaService
            )
        {
            _customLogRepository = customLogRepository ?? throw new ArgumentNullException(nameof(customLogRepository));
            _tygaService = tygaService ?? throw new ArgumentNullException(nameof(tygaService));
            _ziplingoEngagementRepository = ziplingoEngagementRepository ?? throw new ArgumentNullException(nameof(ziplingoEngagementRepository));
        }

        [HttpPost]
        [Route("DbCopyAfter")]
        public IActionResult DbCopyAfter()
        {
            try
            {
                _ziplingoEngagementRepository.ResetSettings();
                return new Responses().OkResult(1);
            }
            catch (Exception e)
            {
                _customLogRepository.CustomErrorLog(0, 0, $"An error occurred with in DbCopy Function", $"Error :  {e.Message}");
                return new Responses().BadRequestResult(e.Message);
            }
        }

        [HttpPost]
        [Route("Tyga/TygaPaymentResponse")]
        public async Task<IActionResult> TygaPaymentResponse([FromBody] TygaPaymentResponse request) 
        {
            try
            {
                await _tygaService.SaveErrorLogResponse(00, 0, "TygaPaymentResponse Start", "Request:- " + Newtonsoft.Json.JsonConvert.SerializeObject(request));
                var status = _tygaService.UpdateOrderStatus(request);
                await _tygaService.SaveErrorLogResponse(00, 0, "TygaPaymentResponse end", "Request:- " + Newtonsoft.Json.JsonConvert.SerializeObject(request) + "Status:- " + status.ToString());
                return new Responses().OkResult();
            }
            catch (Exception ex)
            {
                await _tygaService.SaveErrorLogResponse(00, 0, "An error occurred with in Tyga Payment response", "Request:- " + Newtonsoft.Json.JsonConvert.SerializeObject(ex) + "Status:- " + ex.Message);
                return new Responses().BadRequestResult(ex.Message);
            }
        }

        [HttpPost]
        [Route("Tyga/CreateOrder")]
        public async Task<IActionResult> CreateOrder([FromBody] CreateTygaOrderRequest request)
        { 
            try
            {
                CreateTygaOrderRequest createTygaOrerRequest = new CreateTygaOrderRequest
                {
                    orderNumber = "TEST-ORDER-501",
                    amount = "1",
                    notifyUrl = "https://tenants-v1-webhooks-test-rdqehkur6a-uc.a.run.app/",
                    returnUrl = "https://google.com/"
                };
                var response = _tygaService.CreateOrder("/orders", createTygaOrerRequest);
                return new Responses().OkResult(response);
            }
            catch (Exception ex)
            {
                return new Responses().BadRequestResult(ex.Message);
            }
        }
    }
}