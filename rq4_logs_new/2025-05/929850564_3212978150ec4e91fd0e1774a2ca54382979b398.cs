using System.ComponentModel.DataAnnotations;
using ClientNexus.API.Utilities;
using ClientNexus.Application.DTO;
using ClientNexus.Application.Interfaces;
using ClientNexus.Domain.Entities.Others;
using ClientNexus.Domain.Exceptions.ServerErrorsExceptions;
using ClientNexus.Domain.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace ClientNexus.API.Controllers
{
    [ApiController]
    [Route("api/notifications")]
    public class NotificationController : ControllerBase
    {
        private readonly INotificationService _notificationService;

        public NotificationController(INotificationService notificationService)
        {
            _notificationService = notificationService;
        }

        [HttpGet("{id}")]
        [Authorize]
        public async Task<IActionResult> GetNotificationById([FromRoute] [Required] string id)
        {
            if (string.IsNullOrEmpty(id))
            {
                throw new InvalidInputException("id can't be null or empty");
            }

            Ulid ulid;
            if (!Ulid.TryParse(id, out ulid))
            {
                throw new InvalidInputException("Invalid Ulid");
            }

            NotificationDTO? notification = await _notificationService.GetNotificationByIdAsync(
                ulid
            );
            if (notification is null)
            {
                return NotFound();
            }

            if (User.GetId()!.Value != notification.UserId)
            {
                return Unauthorized();
            }

            return Ok(notification);
        }

        [HttpGet]
        [Authorize]
        public async Task<IActionResult> GetUserNotifications(
            [FromQuery] string? belowId = null,
            [FromQuery] int limit = 10
        )
        {
            int? userId = User.GetId();
            if (userId is null)
            {
                return Unauthorized();
            }

            Ulid? ulid = null;
            if (belowId is not null)
            {
                if (Ulid.TryParse(belowId, out var tmp))
                {
                    ulid = tmp;
                }
                else
                {
                    throw new InvalidInputException("Invalid Ulid");
                }
            }

            return Ok(
                await _notificationService.GetLatestUserNotificationsAsync(
                    userId.Value,
                    limit,
                    ulid
                )
            );
        }
    }
}