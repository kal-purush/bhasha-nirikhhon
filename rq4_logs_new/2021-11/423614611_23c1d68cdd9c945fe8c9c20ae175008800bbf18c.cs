using church_mgt_core.services.interfaces;
using church_mgt_dtos.PaymentDtos;
using church_mgt_dtos.PaymentTypeDtos;
using church_mgt_models;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using PayStack.Net;
using System.Threading.Tasks;

namespace church_mgt_api.Controllers
{
    [ApiController]
    [Route ("api/[controller]")]
    public class PaymentsController : ControllerBase
    {
        private readonly IPaymentTypeService _paymentTypeService;
        private readonly IPaymentService _paymentService;
        private readonly UserManager<AppUser> _userManager;
        private readonly IConfiguration _configuration;

        private PayStackApi PayStack { get; set; }

        public PaymentsController(
            IPaymentTypeService paymentTypeService, 
            IPaymentService paymentService, 
            UserManager<AppUser> userManager,
            IConfiguration configuration)
        {
            _paymentTypeService = paymentTypeService;
            _paymentService = paymentService;
            _userManager = userManager;
            _configuration = configuration;

            PayStack = new PayStackApi(_configuration["Payment:PaystackSK"]);
        }

        [HttpGet()]
        public async Task<IActionResult> GetAllPayments()
        {
            var result = await _paymentService.GetAllPaymentsAsync();
            return StatusCode(result.StatusCode, result);
        }

        [HttpGet("{paymentId}")]
        public async Task<IActionResult> GetPayments(string paymentId)
        {
            var result = await _paymentService.GetPaymentByIdAsync(paymentId);
            return StatusCode(result.StatusCode, result);
        }


        [HttpPost()]
        [Authorize]
        public async Task<IActionResult> MakePayment(MakePaymentDto payment)
        {
            var user = await _userManager.GetUserAsync(User);
            var result = await _paymentService.MakePaymentAsync(user.Id, payment);

            if (result.Succeeded)
                return StatusCode(result.StatusCode, result);

            return StatusCode(result.StatusCode, result.Data.Message);            
        }

        [HttpGet("VerifyPayment")]
        public async Task<IActionResult> Verify(string trxref, string reference)
        {
            var result = await _paymentService.VerifyPaymentAsync(reference);
            return StatusCode(result.StatusCode, result);
        }



        // ======================== PaymentType Controller======================
        [HttpGet("AllPaymentTypes")]
        public IActionResult GetAllPaymentType()
        {
            var result = _paymentTypeService.GetAllPaymentType();
            return StatusCode(result.StatusCode, result);
        }

        [HttpGet("PaymentTypes/{paymentTypeId}")]
        public async Task<IActionResult> GetPaymentType(string paymentTypeId)
        {
            var result = await _paymentTypeService.GetPaymentTypeById(paymentTypeId);
            return StatusCode(result.StatusCode, result);
        }

        [HttpPost("PaymentTypes")]
        public async Task<IActionResult> PaymentType(AddPaymentTypeDto paymentTypeDto)
        {
            var result = await _paymentTypeService.AddPaymentType(paymentTypeDto);
            return StatusCode(result.StatusCode, result);
        }

        [HttpDelete("PaymentTypes/{paymentTypeId}")]
        public async Task<IActionResult> DeletePaymentType(string paymentTypeId)
        {
            var result = await _paymentTypeService.DeletePaymentType(paymentTypeId);
            return StatusCode(result.StatusCode, result);
        }

        [HttpPut("PaymentTypes/{paymentTypeId}")]
        public async Task<IActionResult> UpdatePaymentType(string paymentTypeId)
        {
            var result = await _paymentTypeService.UpdatePaymentType(paymentTypeId);
            return StatusCode(result.StatusCode, result);
        }
    }
}