using System.Security.Claims;
using MediatR;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Vulder.School.Form.Core.Models;

namespace Vulder.School.Form.Api.Controllers.Form;

[Authorize]
[ApiController]
[Route("/form/[controller]")]
public class RefuseSchoolFormController : ControllerBase
{
    private readonly IMediator _mediator;
    
    public RefuseSchoolFormController(IMediator mediator)
    {
        _mediator = mediator;
    }

    [HttpPut]
    public async Task<IActionResult> RefuseForm([FromBody] SchoolFormRefuseModel refuseModel)
    {
        await _mediator.Send(new SchoolFormRefuseRequestModel
        {
            FormId = refuseModel.FormId,
            AdminId = Guid.Parse(User.FindFirst(ClaimTypes.PrimarySid)?.Value!)
        });

        return Ok();
    }
}