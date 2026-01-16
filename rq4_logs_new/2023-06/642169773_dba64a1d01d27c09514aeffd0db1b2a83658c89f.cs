using FBus_BE.DTOs;
using FBus_BE.Models;
using FBus_BE.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics.CodeAnalysis;

namespace FBus_BE.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class AccountController : ControllerBase
    {
        private readonly IAccountService _accountService;
        public AccountController(IAccountService accountService)
        {
            _accountService = accountService;
        }

        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(PageResponse<AccountDTO>))]
        [HttpGet]
        public async Task<IActionResult> GetAccountList([FromQuery] PageRequest pageRequest)
        {
            PageResponse<AccountDTO> pageAccount = await _accountService.GetAccountsWithPaging(pageRequest);
            return Ok(pageAccount);
        }

        [ProducesResponseType(StatusCodes.Status200OK, Type = typeof(AccountDTO))]
        [HttpGet("{id:int}")]
        public async Task<IActionResult> GetAccountDetails([FromRoute] int id)
        {
            AccountDTO account = await _accountService.GetAccountDetails(id);
            return Ok(account);
        }
    }
}