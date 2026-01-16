using LT.DigitalOffice.Kernel.Enums;
using LT.DigitalOffice.Kernel.FluentValidationExtensions;
using LT.DigitalOffice.Kernel.Helpers.Interfaces;
using LT.DigitalOffice.Kernel.Responses;
using LT.DigitalOffice.UserService.Business.Commands.Password.Interfaces;
using LT.DigitalOffice.UserService.Business.Helpers.Password;
using LT.DigitalOffice.UserService.Data.Interfaces;
using LT.DigitalOffice.UserService.Models.Db;
using LT.DigitalOffice.UserService.Models.Dto;
using LT.DigitalOffice.UserService.Models.Dto.Requests.Credentials.Filters;
using LT.DigitalOffice.UserService.Validation.Password.Interfaces;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

namespace LT.DigitalOffice.UserService.Business.Commands.Password
{
  public class ReconstructPasswordCommand : IReconstructPasswordCommand
  {
    private readonly IReconstructPassordRequestValidator _validator;
    private readonly IUserCredentialsRepository _repository;
    private readonly IResponseCreator _responseCreator;

    public ReconstructPasswordCommand(
      IReconstructPassordRequestValidator validator,
      IUserCredentialsRepository repository,
      IResponseCreator responseCreator)
    {
      _validator = validator;
      _repository = repository;
      _responseCreator = responseCreator;
    }

    public async Task<OperationResultResponse<bool>> ExecuteAsync(ReconstructPasswordRequest request)
    {
      if (!_validator.ValidateCustom(request, out List<string> errors))
      {
        return _responseCreator.CreateFailureResponse<bool>(HttpStatusCode.BadRequest, errors);
      }

      DbUserCredentials dbUserCredentials = await _repository.GetAsync(new GetCredentialsFilter() { UserId = request.UserId });

      if (dbUserCredentials is null)
      {
        _responseCreator.CreateFailureResponse<bool>(
          HttpStatusCode.BadRequest,
          new List<string> { "The user credentials was not found." });
      }

      dbUserCredentials.Salt = $"{Guid.NewGuid()}{Guid.NewGuid()}";
      dbUserCredentials.PasswordHash = UserPasswordHash.GetPasswordHash(
        request.Login,
        dbUserCredentials.Salt,
        request.NewPassword);

      OperationResultResponse<bool> response = new();

      response.Body = await _repository.EditAsync(dbUserCredentials);
      response.Status = OperationResultStatusType.FullSuccess;

      if (!response.Body)
      {
        response = _responseCreator.CreateFailureResponse<bool>(HttpStatusCode.BadRequest);
      }

      return response;
    }
  }
}