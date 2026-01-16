
using System.Net;
using Expenses.Api.DataContracts.Applications;
using Libs.Auth.Helpers;
using Libs.Auth.Models.Config;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace Expenses.Api.Controllers;

[ApiController]
[Route("api/report")]
[Authorize]
public class ReportController : ControllerBase
{

    private readonly IExpenseApplication _expenseApplication;
    private readonly CustomClaimSettings _claimSettings;
    private Guid UserId => UserClaimsHelper.GetUserGuidIdFromClaims(User, _claimSettings);

    public ReportController(IExpenseApplication expenseApplication, IOptions<CustomClaimSettings> claimSettings)
    {
        _expenseApplication = expenseApplication;
        _claimSettings = claimSettings.Value;
    }

    // [HttpGet("expenses")]
    // [ProducesResponseType((int)HttpStatusCode.OK)]
    // [ProducesResponseType((int)HttpStatusCode.InternalServerError)]
    // public async Task<

}