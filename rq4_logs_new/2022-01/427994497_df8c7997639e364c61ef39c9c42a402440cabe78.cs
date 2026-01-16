using MediatR;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Vulder.School.Form.Core.Models;

namespace Vulder.School.Form.Api.Controllers.Admin;

[Authorize]
[ApiController]
[Route("/admin/[controller]")]
public class GetSchoolFormsController : ControllerBase
{
    private readonly IMediator _mediator;
    
    public GetSchoolFormsController(IMediator mediator)
    {
        _mediator = mediator;
    }

    [HttpGet]
    public async Task<IActionResult> GetSchoolForms([FromQuery] int page = 1)
    {
        var result = await _mediator.Send(new SchoolFormCollectionModel
        {
            Page = page
        });
        
        return Ok(result);
    }
}