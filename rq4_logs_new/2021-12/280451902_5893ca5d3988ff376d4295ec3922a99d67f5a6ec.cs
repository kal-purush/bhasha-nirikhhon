using System.Threading.Tasks;
using BudgetUnderControl.Modules.Notifications.Core.Services.NotificationsService;
using BudgetUnderControl.Shared.Abstractions.Events;
using Microsoft.Extensions.Logging;

namespace BudgetUnderControl.Modules.Notifications.Core.Events.External.Users.Handlers
{
   
    internal sealed class UserActivationCodeResetEventHandler: IEventHandler<UserActivationCodeResetEvent>
    {
        private readonly ILogger<UserActivationCodeResetEventHandler> _logger;
        private readonly INotificationsService _notificationService;
        public UserActivationCodeResetEventHandler(ILogger<UserActivationCodeResetEventHandler> logger, INotificationsService notificationService)
        {
            _logger = logger;
            _notificationService = notificationService;
        }
        
        public async Task HandleAsync(UserActivationCodeResetEvent @event)
        {
            _logger.LogInformation($@"Received event about user reset password: {@event.ReceiverEmail}");
            await this._notificationService.SendRegisterNotificationAsync(@event.ReceiverEmail, @event.ReceiverFirstName, @event.ReceiverLastName, @event.ActivationCode);
        }
    }
}