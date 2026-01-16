using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using P2PLoan.DTOs;
using P2PLoan.Helpers;
using P2PLoan.Interfaces.Services;

namespace P2PLoan.Controllers
{
    [ApiController]
    [Route("api/bank")]
    public class BankController: ControllerBase
    {
       
        private readonly IBankService bankService;
        public BankController(IBankService bankService)
        {
            this.bankService = bankService;
        }

        [HttpGet]
        [Route("verify")]
        public async Task<IActionResult> VerifyAccount([FromQuery] VerifyAccountDetailsDto verifyAccountDetailsDto)
        {
            var response = await bankService.VerifyAccountDetails(verifyAccountDetailsDto);
            return ControllerHelper.HandleApiResponse(response);
        }

        [HttpGet]
        [Route("banks")]
        public async Task<IActionResult> GetBanksFromMonify()
        {
            var response = await bankService.GetBanks();
            return ControllerHelper.HandleApiResponse(response);
        }


        
    }
}