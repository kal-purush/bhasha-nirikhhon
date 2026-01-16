using LocalAccountServiceProvider.Data.DTOs;
using LocalAccountServiceProvider.Data.Models;
using LocalAccountServiceProvider.Services;
using Microsoft.AspNetCore.Mvc;

namespace LocalAccountServiceProvider.Controllers
{
    [Route("api/account")]
    [ApiController]
    public class AccountController(IAccountService accountService) : ControllerBase
    {
        private readonly IAccountService _accountService = accountService;

        [HttpPost("create-account")]
        public async Task<AccountServiceResultRest> CreateAccount([FromBody] CreateAccountRequestRest request)
        {
            if (!ModelState.IsValid)
            {
                return new AccountServiceResultRest { Success = false, Error = "Invalid input data" };
            }
            var result = await _accountService.CreateAccount(request);
            return result;
        }

        [HttpGet("find-by-email/{email}")]
        public async Task<AccountServiceResultRest> FindByEmail(string email)
        {
            var result = await _accountService.FindByEmail(new FindByEmailRequestRest { Email = email });
            return result;
        }

        [HttpGet("get-account/{id}")]
        public async Task<AccountResponseRest> GetAccountAsync(string id)
        {
            if (!ModelState.IsValid)
            {
                return new AccountResponseRest { Error = "Invalid data input" };
            }

            var result = await _accountService.GetAccount(new GetAccountRequestRest { Id = id });

            if (result == null)
            {
                return new AccountResponseRest { Error = "Profile not found." };
            }

            return result;
        }
    }
}