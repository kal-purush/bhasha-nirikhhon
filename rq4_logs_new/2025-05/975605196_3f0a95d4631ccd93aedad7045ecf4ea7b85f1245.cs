using FluentValidation;
using Microsoft.AspNetCore.Mvc;
using static dotnet9_ketnoigiaothuong.Domain.Contracts.QuotationResponseContract;

namespace dotnet9_ketnoigiaothuong.Controllers
{
    [ApiController]
    public class QuotationReponseController : BaseController
    {
        private readonly IValidator<CreateQuotationResponse> _validatorCreateQuotationResponse;
        private readonly IValidator<UpdateQuotationResponse> _validatorUpdateQuotationResponse;

        public QuotationReponseController(IValidator<CreateQuotationResponse> validatorCreateQuotationResponse,
            IValidator<UpdateQuotationResponse> validatorUpdateQuotationResponse)
        {
            _validatorCreateQuotationResponse = validatorCreateQuotationResponse;
            _validatorUpdateQuotationResponse = validatorUpdateQuotationResponse;
        }

        [HttpPost("api/quotation-response")]
        public async Task<IActionResult> CreateQuotationResponse([FromBody] CreateQuotationResponse request)
        {
            var validationResult = await _validatorCreateQuotationResponse.ValidateAsync(request);
            if (!validationResult.IsValid)
            {
                return BadRequest(validationResult.Errors);
            }
            var response = await Provider.QuotationResponseService.CreateQuotationResponse(request);
            return Ok(response);
        }

        [HttpPut("api/quotation-response/{id}")]
        public async Task<IActionResult> UpdateQuotationResponse(int id, [FromBody] UpdateQuotationResponse request)
        {
            var validationResult = await _validatorUpdateQuotationResponse.ValidateAsync(request);
            if (!validationResult.IsValid)
            {
                return BadRequest(validationResult.Errors);
            }
            var response = await Provider.QuotationResponseService.UpdateQuotationResponse(id, request);
            return Ok(response);
        }
    }
}