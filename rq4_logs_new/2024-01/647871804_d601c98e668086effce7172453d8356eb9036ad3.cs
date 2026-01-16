using MeowLib.Domain.Character.Dto;
using MeowLib.Domain.Character.Services;
using MeowLib.Domain.File.Dto;
using MeowLib.Domain.Shared.Exceptions;
using MeowLib.Domain.User.Enums;
using MeowLib.WebApi.Abstractions;
using MeowLib.WebApi.Filters;
using MeowLib.WebApi.Models.Requests.v1.Character;
using MeowLib.WebApi.Models.Responses.v1.Character;
using MeowLib.WebApi.ProducesResponseTypes;
using Microsoft.AspNetCore.Mvc;

namespace MeowLib.WebApi.Controllers.v1;

[Route("api/v1/character")]
public class CharacterController(ICharacterService characterService, ILogger<CharacterController> logger)
    : BaseController
{
    /// <summary>
    /// Создание нового персонажа.
    /// </summary>
    /// <param name="payload">Данные.</param>
    [HttpPost]
    [Authorization(RequiredRoles = new[] { UserRolesEnum.Admin, UserRolesEnum.Editor })]
    [ProducesOkResponseType(typeof(CreateCharacterResponse))]
    [ProducesForbiddenResponseType]
    public async Task<IActionResult> CreateCharacter([FromBody] CreateCharacterRequest payload)
    {
        var createCharacterResult = await characterService.CreateCharacterAsync(new CharacterDto
        {
            Id = 0,
            Name = payload.Name,
            Description = payload.Description,
            Image = new FileShortDto
            {
                Id = payload.ImageId
            }
        });

        if (createCharacterResult.IsFailure)
        {
            var exception = createCharacterResult.GetError();
            if (exception is ValidationException validationException)
            {
                return ValidationError(validationException.ValidationErrors);
            }

            logger.LogError("Ошибка создания нового персонажа: {exception}", exception);
            return ServerError();
        }

        return Ok(new CreateCharacterResponse
        {
            CreatedId = createCharacterResult.GetResult().Id
        });
    }
}