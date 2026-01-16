using Application.Tasks.Commands;
using Application.Tasks.Queries;
using MediatR;
using Microsoft.AspNetCore.Mvc;
 
using MediatR;
using Microsoft.AspNetCore.Mvc;
using Application.Tasks.Queries;
using Application.Tasks.Commands;
using Infrastructure.Services;

namespace API.Controllers;

[ApiController]
[Route("api/Notifications")]
public class NotificationController(IMediator mediator) : Controller { 
    private readonly IMediator _mediator = mediator;


    [HttpGet("getUnread/{studentId}")]
    public IActionResult GetUnreadNotifications(int studentId)
    {
        return Ok();
    }

}