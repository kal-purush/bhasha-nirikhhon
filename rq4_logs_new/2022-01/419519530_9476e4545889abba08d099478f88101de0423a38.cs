using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FluentValidation;
using LT.DigitalOffice.Kernel.BrokerSupport.Broker;
using LT.DigitalOffice.Models.Broker.Common;
using LT.DigitalOffice.PositionService.Data.Interfaces;
using LT.DigitalOffice.PositionService.Models.Dto.Requests.PositionUser;
using LT.DigitalOffice.PositionService.Validation.PositionUser.Interfaces;
using MassTransit;
using Microsoft.Extensions.Logging;

namespace LT.DigitalOffice.PositionService.Validation.PositionUser
{
  public class EditPositionUserRequestValidator : AbstractValidator<EditPositionUserRequest>, IEditPositionUserRequestValidator
  {
    IRequestClient<ICheckUsersExistence> _rcCheckUsersExistence;
    private readonly ILogger<EditPositionUserRequestValidator> _logger;

    private async Task<bool> CheckUsersExistenceAsync(List<Guid> usersIds)
    {
      try
      {
        Response<IOperationResult<ICheckUsersExistence>> response =
          await _rcCheckUsersExistence.GetResponse<IOperationResult<ICheckUsersExistence>>(
            ICheckUsersExistence.CreateObj(usersIds));

        if (response.Message.IsSuccess)
        {
          return usersIds.Count == response.Message.Body.UserIds.Count;
        }

        _logger.LogWarning(
          "Error while find users Ids: {UsersIds}.\n{Errors}:",
          string.Join('\n', usersIds),
          string.Join('\n', response.Message.Errors));
      }
      catch (Exception exc)
      {
        _logger.LogError(
          exc,
          "Cannot check existing users ids: {UsersIds}",
          string.Join('\n', usersIds));
      }

      return false;
    }

    public EditPositionUserRequestValidator(
      IPositionRepository positionRepository,
      IRequestClient<ICheckUsersExistence> rcCheckUsersExistence,
      ILogger<EditPositionUserRequestValidator> logger)
    {
      _rcCheckUsersExistence = rcCheckUsersExistence;
      _logger = logger;

      RuleFor(request => request)
        .MustAsync(async (request, _) => 
          !await CheckUsersExistenceAsync(new List<Guid>() { request.UserId }))
        .WithMessage("This user's position cannot be changed.");

      When(request =>
        request.PositionId.HasValue,
        () =>
          RuleFor(request => request.PositionId.Value)
            .MustAsync(async (id, _) => !await positionRepository.DoesExistAsync(id))
            .WithMessage("Position must exist."));
    }
  }
}