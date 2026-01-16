using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using P2PLoan.Attributes;
using P2PLoan.DTOs;
using P2PLoan.DTOs.SearchParams;
using P2PLoan.Helpers;
using P2PLoan.Interfaces;

namespace P2PLoan.Controllers;

[ApiController]
[Route("api/loan")]
[Authorize]
public class LoanController
{
    private readonly ILoanService loanService;
    public LoanController(ILoanService LoanService)
    {
        this.loanService = LoanService;
    }

    [HttpGet]
    public async Task<IActionResult> GetUserLoans([FromQuery] LoanSearchParams loanSearchParams)
    {
        var response = await loanService.GetMyLoans(loanSearchParams);
        return ControllerHelper.HandleApiResponse(response);

    }

    [HttpGet("{loanId:guid}")]
    public async Task<IActionResult> GetALoan(Guid loanId)
    {
        var response = await loanService.GetALoan(loanId);
        return ControllerHelper.HandleApiResponse(response);
    }

    [HttpGet("active")]
    public async Task<IActionResult> GetActiveLoan()
    {
        var response = await loanService.GetActiveLoan();
        return ControllerHelper.HandleApiResponse(response);
    }

    [HttpGet("{loanId:guid}/repayments")]
    public async Task<IActionResult> GetLoanRepayments(Guid loanId, [FromQuery] RepaymentSearchParams repaymentSearchParams)
    {
        var response = await loanService.GetLoanRepayments(loanId, repaymentSearchParams);
        return ControllerHelper.HandleApiResponse(response);
    }

    [HttpGet("repayments/{repaymentId:guid}")]
    public async Task<IActionResult> GetARepayment(Guid repaymentId)
    {
        var response = await loanService.GetALoanRepayment(repaymentId);
        return ControllerHelper.HandleApiResponse(response);
    }

    [HttpPost("repay")]
    [TypeFilter(typeof(RequiresPinAttribute))]
    public async Task<IActionResult> RepayLoan([FromBody] RepayLoanRequestDto repayLoanRequestDto)
    {
        var response = await loanService.RepayLoan(repayLoanRequestDto);
        return ControllerHelper.HandleApiResponse(response);
    }

}