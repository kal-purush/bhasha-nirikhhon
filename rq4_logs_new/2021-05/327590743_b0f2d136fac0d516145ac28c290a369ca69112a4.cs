using MediatR;
using Microsoft.AspNetCore.Mvc;
using SFA.DAS.ApprenticeCommitments.Application.Commands.HowApprenticeshipWillBeDeliveredCommand;
using System;
using System.Threading.Tasks;

namespace SFA.DAS.ApprenticeCommitments.Api.Controllers
{
    public class ConfirmHowApprenticeshipWillBeDeliveredRequest
    {
        public bool HowApprenticeshipDeliveredCorrect { get; set; }
    }

    [ApiController]
    public class ConfirmHowApprenticeshipWillBeDeliveredController : ControllerBase
    {
        private readonly IMediator _mediator;

        public ConfirmHowApprenticeshipWillBeDeliveredController(IMediator mediator) => _mediator = mediator;

        [HttpPost("apprentices/{apprenticeId}/apprenticeships/{apprenticeshipId}/howapprenticeshipwillbedeliveredconfirmation")]
        [HttpPost("apprentices/{apprenticeId}/apprenticeships/{apprenticeshipId}/{commitmentStatementId}/HowApprenticeshipWillBeDeliveredConfirmation")]
        public async Task HowApprenticeshipWillBeDelivered(
            Guid apprenticeId, long apprenticeshipId, long commitmentStatementId,
            [FromBody] ConfirmHowApprenticeshipWillBeDeliveredRequest request)
        {
            await _mediator.Send(new HowApprenticeshipWillBeDeliveredCommand(
                (apprenticeId, apprenticeshipId, commitmentStatementId),
                request.HowApprenticeshipDeliveredCorrect));
        }
    }
}