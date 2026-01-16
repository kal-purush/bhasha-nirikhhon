using Asp.Versioning;
using AutoMapper;
using FluentValidation;
using FluentValidation.Results;
using Map.API.Extension;
using Map.API.Models.TripDto;
using Map.API.Models.UserDto;
using Map.Domain.Entities;
using Map.Domain.ErrorCodes;
using Map.Domain.Models.AuthDto;
using Map.Platform.Interfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using static Map.API.Controllers.Models.HttpError;

namespace Map.API.Controllers;

[ApiController]
[ApiVersion(ApiControllerVersions.LastVersion)]
[Route("api/v{version:apiVersion}/[controller]")]
public class UserController : ControllerBase
{
    #region Props

    private readonly UserManager<MapUser> _userManager;
    private readonly IUserPlatform _userPlatform;
    private readonly IValidator<UpdateUserMailDto> _updateUserMailValidator;
    private readonly IMapper _mapper;

    #endregion

    #region Ctor

    public UserController(ITripPlatform tripPlatform,
                          UserManager<MapUser> userManager,
                          IMapper mapper,
                          IValidator<UpdateUserMailDto> updateUserMailValidator,
                          IUserPlatform userPlatform)
    {
        _userManager = userManager ?? throw new ArgumentNullException(nameof(userManager));
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        _updateUserMailValidator = updateUserMailValidator ?? throw new ArgumentNullException(nameof(updateUserMailValidator));
        _userPlatform = userPlatform ?? throw new ArgumentNullException(nameof(userPlatform));
    }

    #endregion


    /// <summary>
    /// Change user email
    /// </summary>
    /// <param name="userId">Id of user</param>
    /// <param name="updateUserMailDto">UpdateUserMailDto</param>
    /// <returns>MapUserDto with new Mail</returns>
    [Authorize]
    [HttpPatch]
    [Route("{userId}/email")]
    [MapToApiVersion(ApiControllerVersions.LastVersion)]
    [ProducesResponseType(typeof(TripDto), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(ICollection<Error>), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status401Unauthorized)]
    [ProducesResponseType(typeof(Error), StatusCodes.Status500InternalServerError)]
    public async Task<ActionResult<MapUserDto>> UpdateUserMailAsync([FromRoute] Guid userId, [FromBody] UpdateUserMailDto updateUserMailDto)
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