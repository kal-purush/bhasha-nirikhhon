using System;
using System.Threading;
using System.Threading.Tasks;
using F3.Src.Common;
using F3.Src.DataAccess;
using F3.Src.Models;
using FCommon.Src.FeatureService;
using FCommon.Src.IdGeneration;

namespace F3.Src.BusinessLogic;

public sealed class F3Service : IServiceHandler<F3AppRequestModel, F3AppResponseModel>
{
    private readonly Lazy<IF3Repository> _repository;
    private readonly Lazy<IAppIdGenerator> _idGenerator;

    public F3Service(Lazy<IF3Repository> repository, Lazy<IAppIdGenerator> idGenerator)
    {
        _repository = repository;
        _idGenerator = idGenerator;
    }

    public async Task<F3AppResponseModel> ExecuteAsync(
        F3AppRequestModel request,
        CancellationToken ct
    )
    {
        var isEmailFound = await _repository.Value.DoesEmailExistsAsync(request.Email, ct);
        if (isEmailFound)
        {
            return F3Constant.DefaultResponse.App.EMAIL_ALREADY_EXISTS;
        }

        var isPasswordValid = await _repository.Value.IsPasswordValidAsync(
            request.Email,
            request.Password,
            ct
        );
        if (!isPasswordValid)
        {
            return F3Constant.DefaultResponse.App.PASSWORD_IS_INVALID;
        }

        var user = CreateNewUser(request);
        var result = await _repository.Value.CreateUserAsync(user, ct);
        if (!result)
        {
            return F3Constant.DefaultResponse.App.SERVER_ERROR;
        }

        return new() { AppCode = F3Constant.AppCode.SUCCESS };
    }

    private F3UserInfoModel CreateNewUser(F3AppRequestModel appRequest)
    {
        return new()
        {
            Id = _idGenerator.Value.NextId(),
            Email = appRequest.Email,
            Password = appRequest.Password,
            EmailConfirmed = true,
            AdditionalUserInfo = new()
            {
                FirstName = string.Empty,
                LastName = string.Empty,
                Description = string.Empty,
            },
        };
    }
}