using CaptoneProject_IOTS_Service.Services.Interface;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace CaptoneProject_IOTS_API.Controllers.NotificationController
{
    [Route("api/notification")]
    [ApiController]
    public class NotificationController : MyBaseController.MyBaseController
    {
        private readonly INotificationService notificationService;
        public NotificationController(INotificationService notificationService)
        {
            this.notificationService = notificationService;
        }

        [HttpGet("get-all")]
        [Authorize]
        public async Task<IActionResult> GetAllNotifications()
        {
            var res = await notificationService.GetNotifications();

            return GetActionResult(res);
        }

        [HttpGet("count-not-read-notification")]
        [Authorize]
        public async Task<IActionResult> CountNotReadNotification()
        {
            var res = await notificationService.CountNotReadNotification();

            return GetActionResult(res);
        }
    }
}