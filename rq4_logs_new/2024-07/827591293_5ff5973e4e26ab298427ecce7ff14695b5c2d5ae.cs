using Journey.Application.UseCases.Participants.Confirm;
using Journey.Communication.Responses;
using Microsoft.AspNetCore.Mvc;

namespace Journey.Api.Controllers;
[Route("api/[controller]")]
[ApiController]
public class ParticipantsController : ControllerBase
{
    [HttpPatch]
    [Route("{participantId}/Confirm")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(typeof(ResponseErrorsJson), StatusCodes.Status404NotFound)]
    public IActionResult Confirm([FromRoute] Guid participantId)
    {
        var useCase = new ConfirmParticipantUseCase();

        useCase.Execute(participantId);

        return NoContent();
    }
}