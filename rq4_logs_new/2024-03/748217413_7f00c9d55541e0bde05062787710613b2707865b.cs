using Asp.Versioning;
using AutoMapper;
using FluentValidation;
using FluentValidation.Results;
using Map.API.Extension;
using Map.Domain.Entities;
using Map.Domain.ErrorCodes;
using Map.Domain.Models.Step;
using Map.Platform.Interfaces;
using Microsoft.AspNetCore.Mvc;
using static Map.API.Controllers.Models.HttpError;

namespace Map.API.Controllers;

[ApiController]
[ApiVersion(ApiControllerVersions.V1)]
[Route("api/v{version:apiVersion}/[controller]")]
public class StepController : ControllerBase
{
    #region Props
    private readonly IMapper _mapper;
    private readonly IValidator<AddStepDto> _addStepValidator;
    private readonly ITripPlatform _tripPlatform;
    private readonly IStepPlatform _stepPlatform;

    #endregion

    #region Ctor
    public StepController(IMapper mapper, IValidator<AddStepDto> addStepValidator, ITripPlatform tripPlatform, IStepPlatform stepPlatform)
    {
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        _addStepValidator = addStepValidator ?? throw new ArgumentNullException(nameof(addStepValidator));
        _tripPlatform = tripPlatform ?? throw new ArgumentNullException(nameof(tripPlatform));
        _stepPlatform = stepPlatform ?? throw new ArgumentNullException(nameof(stepPlatform));
    }
    #endregion

    /// <summary>
    /// Add a step to a trip to end of steps in trip
    /// </summary>
    /// <param name="tripId">Trip id to add Step</param>
    /// <param name="addStepDto">addStepDto</param>
    //[Authorize]
    [HttpPost]
    [Route("{tripId}")]
    [MapToApiVersion(ApiControllerVersions.V1)]
    [ProducesResponseType(typeof(StepDto), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ICollection<Error>), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status401Unauthorized)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status500InternalServerError)]
    public async Task<ActionResult<StepDto>> AddStepAsync([FromRoute] Guid tripId, [FromBody] AddStepDto addStepDto)
    {
        ValidationResult validationResult = _addStepValidator.Validate(addStepDto);

        if (!validationResult.IsValid)
        {
            return BadRequest(validationResult.Errors.Select(e => new Error(e.ErrorCode, e.ErrorMessage)));
        }

        Trip? trip = await _tripPlatform.GetTripByIdAsync(tripId);
        if (trip is null)
            return NotFound(new Error(ETripErrorCodes.TripNotFoundById.ToStringValue(), "Voyage non trouvé par id"));

        Step entity = _mapper.Map<AddStepDto, Step>(addStepDto);
        await _stepPlatform.AddStepAsync(trip, entity);

        return CreatedAtAction(nameof(GetStepByIdAsync), new { stepId = entity.StepId }, _mapper.Map<Step, StepDto>(entity));
    }

    /// <summary>
    /// Add a step to a trip to end of steps in trip
    /// </summary>
    /// <param name="tripId">Trip id to add Step</param>
    /// <param name="addStepDto">addStepDto</param>
    //[Authorize]
    [HttpPost]
    [Route("{tripId}/before-{stepId}")]
    [MapToApiVersion(ApiControllerVersions.V1)]
    [ProducesResponseType(typeof(StepDto), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ICollection<Error>), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status401Unauthorized)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status500InternalServerError)]
    public async Task<ActionResult<StepDto>> AddStepBeforAsync([FromRoute] Guid tripId, [FromRoute] Guid stepId, [FromBody] AddStepDto addStepDto)
    {
        ValidationResult validationResult = _addStepValidator.Validate(addStepDto);

        if (!validationResult.IsValid)
        {
            return BadRequest(validationResult.Errors.Select(e => new Error(e.ErrorCode, e.ErrorMessage)));
        }

        Trip? trip = await _tripPlatform.GetTripByIdAsync(tripId);
        if (trip is null)
            return NotFound(new Error(ETripErrorCodes.TripNotFoundById.ToStringValue(), "Voyage non trouvé par id"));

        Step? nextStep = trip.Steps.FirstOrDefault(s => s.StepId == stepId);
        if (nextStep is null)
            return NotFound(new Error(ETripErrorCodes.StepNotFoundById.ToStringValue(), "Etape non trouvé par id"));

        Step entity = _mapper.Map<AddStepDto, Step>(addStepDto);
        await _stepPlatform.AddStepBeforAsync(trip, nextStep, entity);

        return CreatedAtAction(nameof(GetStepByIdAsync), new { stepId = entity.StepId }, _mapper.Map<Step, StepDto>(entity));
    }

    /// <summary>
    /// Add a step to a trip to end of steps in trip
    /// </summary>
    /// <param name="tripId">Trip id to add Step</param>
    /// <param name="addStepDto">addStepDto</param>
    //[Authorize]
    [HttpPost]
    [Route("{tripId}/after-{stepId}")]
    [MapToApiVersion(ApiControllerVersions.V1)]
    [ProducesResponseType(typeof(StepDto), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ICollection<Error>), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status401Unauthorized)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status500InternalServerError)]
    public async Task<ActionResult<StepDto>> AddStepAfterAsync([FromRoute] Guid tripId, [FromRoute] Guid stepId, [FromBody] AddStepDto addStepDto)
    {
        ValidationResult validationResult = _addStepValidator.Validate(addStepDto);

        if (!validationResult.IsValid)
        {
            return BadRequest(validationResult.Errors.Select(e => new Error(e.ErrorCode, e.ErrorMessage)));
        }

        Trip? trip = await _tripPlatform.GetTripByIdAsync(tripId);
        if (trip is null)
            return NotFound(new Error(ETripErrorCodes.TripNotFoundById.ToStringValue(), "Voyage non trouvé par id"));

        Step? previousStep = trip.Steps.FirstOrDefault(s => s.StepId == stepId);
        if (previousStep is null)
            return NotFound(new Error(ETripErrorCodes.StepNotFoundById.ToStringValue(), "Etape non trouvé par id"));

        Step entity = _mapper.Map<AddStepDto, Step>(addStepDto);
        await _stepPlatform.AddStepAfterAsync(trip, previousStep, entity);

        return CreatedAtAction(nameof(GetStepByIdAsync), new { stepId = entity.StepId }, _mapper.Map<Step, StepDto>(entity));
    }

    /// <summary>
    /// Get step by id
    /// </summary>
    /// <param name="stepId">Id of wanted Step</param>
    [HttpGet]
    [Route("{stepId}")]
    [MapToApiVersion(ApiControllerVersions.V1)]
    [ProducesResponseType(typeof(StepDto), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status404NotFound)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status401Unauthorized)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status500InternalServerError)]
    public async Task<ActionResult<StepDto>> GetStepByIdAsync([FromRoute] Guid stepId)
    {
        Step? step = await _stepPlatform.GetStepByIdAsync(stepId);
        if (step is null)
            return NotFound(new Error(EStepErrorCodes.StepNotFoundById.ToStringValue(), "Etape non trouvé par id"));

        return _mapper.Map<Step, StepDto>(step);
    }


}