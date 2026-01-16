using System;
using System.Threading;
using System.Threading.Tasks;
using F16.Common;
using F16.DataAccess;
using F16.Models;
using FCommon.FeatureService;

namespace F16.BusinessLogic;

public sealed class F16Service : IServiceHandler<F16AppRequestModel, F16AppResponseModel>
{
    private readonly Lazy<IF16Repository> _repository;

    public F16Service(Lazy<IF16Repository> repository)
    {
        _repository = repository;
    }

    public async Task<F16AppResponseModel> ExecuteAsync(
        F16AppRequestModel request,
        CancellationToken ct
    )
    {
        var doesTaskExist = await _repository.Value.DoesTodoTaskExistAsync(request.TodoTaskId, ct);
        if (!doesTaskExist)
        {
            return F16Constant.DefaultResponse.App.TASK_NOT_FOUND;
        }

        var isSuccess = await _repository.Value.ModifyIsInMyDayStatusAsync(
            request.TodoTaskId,
            request.IsInMyDay,
            ct
        );
        if (!isSuccess)
        {
            return F16Constant.DefaultResponse.App.SERVER_ERROR;
        }

        return new() { AppCode = F16Constant.AppCode.SUCCESS };
    }
}