using FluentValidation.Results;
using LT.DigitalOffice.Kernel.BrokerSupport.AccessValidatorEngine.Interfaces;
using LT.DigitalOffice.Kernel.Constants;
using LT.DigitalOffice.Kernel.Enums;
using LT.DigitalOffice.Kernel.Extensions;
using LT.DigitalOffice.Kernel.Helpers.Interfaces;
using LT.DigitalOffice.Kernel.Responses;
using LT.DigitalOffice.UserService.Business.Commands.Avatar.Interfaces;
using LT.DigitalOffice.UserService.Data.Interfaces;
using LT.DigitalOffice.UserService.Validation.Image.Interfaces;
using Microsoft.AspNetCore.Http;
using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace LT.DigitalOffice.UserService.Business.Commands.Avatar
{
  public class EditAvatarCommand : IEditAvatarCommand
  {
    private readonly IAvatarRepository _avatarRepository;
    private readonly IAccessValidator _accessValidator;
    private readonly IHttpContextAccessor _httpContextAccessor;
    private readonly IResponseCreator _responseCreator;

    public EditAvatarCommand(
      IHttpContextAccessor httpContextAccessor,
      IAccessValidator accessValidator,
      IResponseCreator responseCreator,
      IAvatarRepository avatarRepository)
    {
      _httpContextAccessor = httpContextAccessor;
      _accessValidator = accessValidator;
      _responseCreator = responseCreator;
      _avatarRepository = avatarRepository;
    }

    public async Task<OperationResultResponse<bool>> ExecuteAsync(Guid userId, Guid imageId)
    {
      if (_httpContextAccessor.HttpContext.GetUserId() != userId
        && !await _accessValidator.HasRightsAsync(Rights.AddEditRemoveUsers))
      {
        return _responseCreator.CreateFailureResponse<bool>(HttpStatusCode.Forbidden);
      }

      OperationResultResponse<bool> response = new();

      response.Body = await _avatarRepository.UpdateCurrentAvatarAsync(userId, imageId);
      response.Status = OperationResultStatusType.FullSuccess;

      if (!response.Body)
      {
        response = _responseCreator.CreateFailureResponse<bool>(HttpStatusCode.NotFound);
      }

      return response;
    }
  }
}