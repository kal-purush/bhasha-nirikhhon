using Application.CQRS.Account.Static;
using Application.CQRS.Announcement.Commands.AddAnnouncement;
using Application.CQRS.Announcement.Commands.DeleteAnnouncement;
using Application.CQRS.Announcement.Dtos;
using Application.CQRS.Announcement.Enum;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace Api.Controllers.Areas.Announcements;

[Route("api/announcement")]
public class AnnouncementController : BaseController
{
    /// <summary>
    /// Add new announcement
    /// </summary>
    /// <param name="command"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    [HttpPost("add-announcement")]
    [Authorize(Roles = UserRoles.Admin)]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public async Task<IActionResult> AddAnnouncement([FromBody] AddAnnouncementCommand command,
        CancellationToken cancellationToken)
    {
        var response = await Mediator.Send(command, cancellationToken);

        return Ok(response);
    }

    /// <summary>
    /// Delete announcement
    /// </summary>
    /// <param name="command"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    [HttpDelete]
    [Authorize(Roles = UserRoles.Admin)]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> DeleteAnnouncement([FromBody] DeleteAnnouncementCommand command,
        CancellationToken cancellationToken)
    {
        var response = await Mediator.Send(command, cancellationToken);

        return Ok(response);
    }

    /// <summary>
    /// Display announcements
    /// </summary>
    /// <param name="query"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    [HttpGet("{State}")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    public async Task<ActionResult<List<GetAnnouncementDto>>> DisplayAnnouncements([FromRoute] State query,
        CancellationToken cancellationToken)
    {
        var response = await Mediator.Send(query, cancellationToken);

        return Ok(response);
    }
}