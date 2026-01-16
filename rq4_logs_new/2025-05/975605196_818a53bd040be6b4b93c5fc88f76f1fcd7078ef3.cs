using FluentValidation;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using static dotnet9_ketnoigiaothuong.Domain.Contracts.CompanyContract;

namespace dotnet9_ketnoigiaothuong.Controllers
{
    [Route("api/company")]
    [ApiController]
    public class CompanyController : BaseController
    {
        private readonly IValidator<CompanyViewModel> _companyValidator;

        public CompanyController(IValidator<CompanyViewModel> companyValidator)
        {
            _companyValidator = companyValidator;
        }
        [HttpPost("create")]
        public async Task<IActionResult> Create([FromBody] CompanyViewModel companyViewModel)
        {
            var validationResult = await _companyValidator.ValidateAsync(companyViewModel);
            if (!validationResult.IsValid)
            {
                return BadRequest(validationResult.Errors);
            }
            var response = await Provider.CompanyService.CreateCompanyProfileAsync(companyViewModel);
            if (response == null)
            {
                return BadRequest("Company already exists");
            }
            return Ok(new {
                response,
                redirect_url = "/"
            });
        }
        [HttpPost("get-by-email-phone")]
        public async Task<IActionResult> GetCompanyProfileByEmailAndPhoneNumber([FromBody] CompanyContact companyContact)
        {
            var response = await Provider.CompanyService.Async_GetCompanyProfileByEmailAndPhoneNumber(companyContact.Email, companyContact.PhoneNumber);
            if (response == null)
            {
                return BadRequest("Company not found!");
            }
            return Ok(response);
        }
    }
}