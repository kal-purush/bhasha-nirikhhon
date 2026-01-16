using Asp.Versioning;
using AutoMapper;
using FluentValidation;
using FluentValidation.Results;
using Map.Domain.Entities;
using Map.Domain.ErrorCodes;
using Map.Domain.Models.AddTravel;
using Map.Domain.Models.Auth;
using Map.Domain.Models.Step;
using Map.Domain.Models.Trip;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Map.API.Controllers;

[ApiController]
[ApiVersion(ApiControllerVersions.V1)]
[Route("api/v{version:apiVersion}/[controller]")]
public class TravelController : ControllerBase
{
    #region Props

    private readonly IMapper _mapper;

    #endregion

    #region Ctor

    public TravelController(IMapper mapper)
    {
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
    }

    #endregion

    [Authorize]
    [HttpPost]
    [Route("origin-{originStepId}/destination-{destinationStepId}")]
    [MapToApiVersion(ApiControllerVersions.V1)]
    [ProducesResponseType(typeof(TripDto), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ICollection<Error>), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status401Unauthorized)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status500InternalServerError)]
    public async Task<ActionResult<StepDto>> UpdateUserMailAsync([FromRoute] int originStepId, [FromRoute] int destinationStepId, [FromBody] AddTravelDto addTravelDto)
    {
        ValidationResult validationResult = await _updateUserMailValidator.ValidateAsync(updateUserMailDto);
        if (!validationResult.IsValid)
            return BadRequest(validationResult.Errors.Select(e => new Error(e.ErrorCode, e.ErrorMessage)));

        MapUser? user = await _userManager.FindByEmailAsync(updateUserMailDto.Mail);
        if (user is null || user.Id != userId)
        {
            return BadRequest(new Error(EMapUserErrorCodes.UserNotFoundByEmail.ToStringValue(), "Utilisateur non trouvé"));
        }

        string changeEmailToken = await _userPlatform.GenerateEmailUpdateTokenAsync(user, updateUserMailDto.Mail);
        IdentityResult? result = await _userPlatform.UpdateEmailAsync(user, updateUserMailDto.Mail, changeEmailToken);

        return Ok(_mapper.Map<MapUser, MapUserDto>(user));
    }

}