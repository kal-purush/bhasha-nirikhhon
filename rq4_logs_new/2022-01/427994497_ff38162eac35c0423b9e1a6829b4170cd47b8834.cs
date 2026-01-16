using MediatR;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Vulder.School.Form.Core.Models;

namespace Vulder.School.Form.Api.Controllers.Form;

[Authorize]
[ApiController]
[Route("/form/[controller]")]
public class GetSchoolFormController : ControllerBase
{
    private readonly IMediator _mediator;
    
    public GetSchoolFormController(IMediator mediator)
    {
        _mediator = mediator;
    }

    [HttpGet]
    public async Task<IActionResult> GetSchoolForm([FromQuery] Guid formId)
    {
        var result = await _mediator.Send(new SchoolFormGetModel
        {
            Id = formId
        });

        return Ok(result);
    }
}