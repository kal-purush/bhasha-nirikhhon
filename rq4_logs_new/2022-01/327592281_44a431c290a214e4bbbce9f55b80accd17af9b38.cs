using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NServiceBus;
using RestEase;
using SFA.DAS.ApprenticeCommitments.Jobs.Api;
using SFA.DAS.CommitmentsV2.Messages.Events;

namespace SFA.DAS.ApprenticeCommitments.Jobs.Functions.EventHandlers.CommitmentsEventHandlers
{
    public class ApprenticeshipResendInvitationEventHandler
        : IHandleMessages<ApprenticeshipResendInvitationEvent>
    {
        private readonly IEcsApi _api;
        private readonly EmailService _emailService;
        private readonly ILogger<ApprenticeshipResendInvitationEventHandler> _logger;

        public ApprenticeshipResendInvitationEventHandler(
            IEcsApi api,
            EmailService emailService,
            ILogger<ApprenticeshipResendInvitationEventHandler> logger)
        {
            _api = api;
            _emailService = emailService;
            _logger = logger;
        }

        public async Task Handle(ApprenticeshipResendInvitationEvent message, IMessageHandlerContext context)
        {
            try
            {
                _logger.LogInformation("Handling ApprenticeshipResendInvitationEvent for {ApprenticeshipId}", message.ApprenticeshipId);
                var registration = await _api.GetApprovalsRegistration(message.ApprenticeshipId);
                if (!registration.ApprenticeId.HasValue)
                {
                    await _emailService.SendApprenticeSignUpInvitation(context, registration.RegistrationId, registration.Email, registration.FirstName);
                }
                else
                {
                    _logger.LogInformation("Commitments Apprenticeship {ApprenticeshipId}, already assigned to an apprentice, invitation email not required", message.ApprenticeshipId);
                }
            }
            catch (ApiException e) when (e.StatusCode == HttpStatusCode.NotFound)
            {
                _logger.LogInformation("Apprenticeship {commitmentsApprenticeshipId} does not exist in domain", message.ApprenticeshipId);
            }

        }

    }
}