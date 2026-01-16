using MeowLib.Domain.People.Dto;
using MeowLib.Domain.People.Services;
using MeowLib.Domain.Shared.Exceptions.Services;
using MeowLib.Domain.User.Enums;
using MeowLib.WebApi.Abstractions;
using MeowLib.WebApi.Filters;
using MeowLib.WebApi.Models.Requests.v1.People;
using MeowLib.WebApi.Models.Responses.v1.People;
using MeowLib.WebApi.ProducesResponseTypes;
using Microsoft.AspNetCore.Mvc;

namespace MeowLib.WebApi.Controllers.v1;

[Route("api/v1/people")]
[ProducesOkResponseType(typeof(PeopleModel))]
[ProducesForbiddenResponseType]
public class PeopleController(IPeopleService peopleService, ILogger<PeopleController> logger) : BaseController
{
    [HttpPost]
    [Authorization(RequiredRoles = new[] { UserRolesEnum.Moderator, UserRolesEnum.Admin })]
    public async Task<IActionResult> CreatePeople([FromBody] CreatePeopleRequest payload)
    {
        var createdPeopleResult = await peopleService.CreatePeopleAsync(payload.Name);
        if (createdPeopleResult.IsFailure)
        {
            var exception = createdPeopleResult.GetError();
            if (exception is ValidationException validationException)
            {
                return ValidationError(validationException.ValidationErrors);
            }

            logger.LogError("Ошибка создания человека: {exception}", exception);
            return ServerError();
        }

        var createdPeople = createdPeopleResult.GetResult();
        return Ok(new PeopleModel
        {
            Id = createdPeople.Id,
            Name = createdPeople.Name
        });
    }

    [HttpPut("{peopleId}")]
    [ProducesOkResponseType(typeof(PeopleModel))]
    [ProducesForbiddenResponseType]
    [ProducesNotFoundResponseType]
    [Authorization(RequiredRoles = new[] { UserRolesEnum.Admin, UserRolesEnum.Moderator })]
    public async Task<IActionResult> UpdatePeople([FromRoute] int peopleId, [FromBody] UpdatePeopleRequest payload)
    {
        var updatePeopleResult = await peopleService.UpdatePeopleAsync(peopleId, new PeopleDto
        {
            Id = peopleId,
            Name = payload.Name
        });
        if (updatePeopleResult.IsFailure)
        {
            var exception = updatePeopleResult.GetError();
            if (exception is ValidationException validationException)
            {
                return ValidationError(validationException.ValidationErrors);
            }

            logger.LogError("Ошибка обновления человека: {exception}", exception);
            return ServerError();
        }

        var updatedPeople = updatePeopleResult.GetResult();
        if (updatedPeople is null)
        {
            return NotFoundError();
        }

        return Ok(new PeopleModel
        {
            Id = updatedPeople.Id,
            Name = updatedPeople.Name
        });
    }

    [HttpGet("{peopleId}")]
    [ProducesOkResponseType(typeof(GetPeopleResponse))]
    [ProducesNotFoundResponseType]
    public async Task<IActionResult> GetPeople([FromRoute] int peopleId)
    {
        var foundedPeople = await peopleService.GetPeopleByIdAsync(peopleId);
        if (foundedPeople is null)
        {
            return NotFoundError();
        }

        return Ok(new GetPeopleResponse
        {
            Id = foundedPeople.Id,
            Name = foundedPeople.Name,
            Books = foundedPeople.BooksPeople.Select(bp => new BookPeopleModel
                {
                    Book = new PeopleBookModel
                    {
                        Id = bp.Book.Id,
                        Name = bp.Book.Name,
                        ImageUrl = bp.Book.Image?.FileSystemName
                    },
                    Role = bp.Role
                })
                .ToList()
        });
    }

    [HttpGet]
    [ProducesOkResponseType(typeof(GetAllPeoplesResponse))]
    public async Task<IActionResult> GetAllPeoples([FromQuery] int page = 1)
    {
        var peoples = await peopleService.GetAllPeoplesWithPageAsync(10, page - 1);
        return Ok(new GetAllPeoplesResponse
        {
            Items = peoples.Select(p => new PeopleModel
                {
                    Id = p.Id,
                    Name = p.Name
                })
                .ToList(),
            Page = page
        });
    }

    [HttpDelete("{peopleId}")]
    [ProducesOkResponseType]
    [ProducesNotFoundResponseType]
    public async Task<IActionResult> DeletePeople([FromRoute] int peopleId)
    {
        var deletePeopleResult = await peopleService.DeletePeopleAsync(peopleId);
        if (deletePeopleResult.IsFailure)
        {
            var exception = deletePeopleResult.GetError();
            logger.LogError("Ошибка удаления человека: {exception}", exception);
            return ServerError();
        }

        var isFounded = deletePeopleResult.GetResult();
        if (!isFounded)
        {
            return NotFoundError();
        }

        return Ok();
    }
}