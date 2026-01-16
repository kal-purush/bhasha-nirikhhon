using FoodBackend.UseCases.Common.Dtos;
using FoodBackend.UseCases.CourseMeal.AddCourseMealToDay;
using FoodBackend.UseCases.CourseMeal.CreateCourseMealDay;
using FoodBackend.UseCases.CourseMeal.GetAllCourseMealDay;
using FoodBackend.UseCases.CourseMeal.GetCourseMealDayById;
using FoodBackend.UseCases.CourseMeal.RemoveCourseMealDayById;
using MediatR;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Saritasa.Tools.Common.Pagination;

namespace GymBackend.Web.Controllers;

/// <summary>
/// Course meal day controller.
/// </summary>
[ApiController]
[Route("api/coursemealday")]
[ApiExplorerSettings(GroupName = "CourseMealDays")]
[Authorize]
public class CourseMealDayController
{
    private readonly IMediator mediator;

    /// <summary>
    /// Constructor.
    /// </summary>
    public CourseMealDayController(IMediator mediator)
    {
        this.mediator = mediator;
    }

    /// <summary>
    /// Get all course meal days.
    /// </summary>
    /// <param name="query">Query.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Paged list.</returns>
    [HttpGet]
    public async Task<PagedListMetadataDto<LightCourseMealDayDto>> GetAllCourseMealDays([FromQuery] GetAllCourseMealDayQuery query, CancellationToken cancellationToken)
        => await mediator.Send(query, cancellationToken);

    /// <summary>
    /// Get course meal day by id.
    /// </summary>
    /// <param name="courseMealDayId">Course meal day id.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    [HttpGet("{courseMealDayId}")]
    public async Task<LightCourseMealDayDto> GetCourseMealDayById(Guid courseMealDayId, CancellationToken cancellationToken)
        => await mediator.Send(new GetCourseMealDayByIdQuery(courseMealDayId), cancellationToken);

    /// <summary>
    /// Create new course meal day.
    /// </summary>
    /// <param name="command">Create course meal day command.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    [HttpPost]
    public async Task<Guid> CreateCourseMealDay(CreateCourseMealDayCommand command, CancellationToken cancellationToken)
        => await mediator.Send(command, cancellationToken);

    /// <summary>
    /// Add course meal to day.
    /// </summary>
    /// <param name="command">Command.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="courseMealDayId">Course meal day id.</param>
    [HttpPut("{courseMealDayId}")]
    public async Task AddCourseMealToDay(Guid courseMealDayId, AddCourseMealToDayCommand command, CancellationToken cancellationToken)
        => await mediator.Send(command with { CourseMealDayId = courseMealDayId }, cancellationToken);

    /// <summary>
    /// Delete course meal day.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="courseMealDayId">Course meal day id.</param>
    [HttpDelete("{courseMealDayId}")]
    public async Task RemoveCourseMealDay(Guid courseMealDayId, CancellationToken cancellationToken)
        => await mediator.Send(new RemoveCourseMealDayByIdCommand(courseMealDayId), cancellationToken);
}