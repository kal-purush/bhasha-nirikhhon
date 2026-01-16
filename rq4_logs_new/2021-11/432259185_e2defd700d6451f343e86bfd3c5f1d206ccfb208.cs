using Microsoft.AspNetCore.Mvc;
using IncidentServer.Services;

namespace IncidentServer.Controllers;

[ApiController]
[Route("[controller]")]
public class NotificationsController : ControllerBase
{
    private readonly ILogger<NotificationsController> _logger;
    private readonly INotificationService _notificationService;
    private readonly IConfiguration _configuration;

    public NotificationsController(ILogger<NotificationsController> logger, INotificationService notificationService, IConfiguration configuration)
    {
        _logger = logger;
        _notificationService = notificationService;
        _configuration = configuration;
    }

    [HttpGet(Name = "GetNotifications")]
    public async Task<IActionResult> Get()
    {
        try 
        {
            return Ok(await _notificationService.GetNotifications());
        }
        catch (Exception ex) 
        {
            return StatusCode(StatusCodes.Status500InternalServerError, ex.Message);
        }
    }
    
}