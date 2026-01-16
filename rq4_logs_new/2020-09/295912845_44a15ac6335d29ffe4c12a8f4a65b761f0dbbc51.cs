using AutoMapper;
using Microsoft.AspNetCore.Mvc;
using OffiRent.API.Domain.Models;
using OffiRent.API.Domain.Services;
using OffiRent.API.Resources.Account;
using Swashbuckle.AspNetCore.Annotations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace OffiRent.API.Controllers
{
    public class AccountController : ControllerBase
    {

        private readonly IAccountService _accountService;
        private readonly IMapper _mapper;

        public AccountController(IAccountService accountService, IMapper mapper)
        {
            _accountService = accountService;
            _mapper = mapper;
        }

        [SwaggerOperation(
           Summary = "Update Premium Status of Account"

       )]
        [SwaggerResponse(200, "Change type of OffiProvider account to Premium", typeof(AccountResource))]
        [HttpPatch("id")]
        public async Task<IActionResult> UpdateAsync(int id)
        {
            var result = await _accountService.UpdateSync(id);

            if (!result.Success)
                return BadRequest(result.Message);
            var accountResource = _mapper.Map<Account, AccountResource>(result.Resource);
            return Ok(accountResource);
        }
    }
}