using System;
using System.Threading;
using System.Threading.Tasks;
using F17.Common;
using F17.DataAccess;
using F17.Models;
using FCommon.FeatureService;

namespace F17.BusinessLogic;

public sealed class F17Service : IServiceHandler<F17AppRequestModel, F17AppResponseModel>
{
    private readonly Lazy<IF17Repository> _repository;

    public F17Service(Lazy<IF17Repository> repository)
    {
        _repository = repository;
    }

    public async Task<F17AppResponseModel> ExecuteAsync(
        F17AppRequestModel request,
        CancellationToken ct
    )
    {
        var doesTaskExist = await _repository.Value.DoesTodoTaskExistAsync(request.TodoTaskId, ct);
        if (!doesTaskExist)
        {
            return F17Constant.DefaultResponse.App.TASK_NOT_FOUND;
        }

        var isSuccess = await _repository.Value.ModifyCompletedStatusAsync(
            request.TodoTaskId,
            request.IsCompleted,
            ct
        );
        if (!isSuccess)
        {
            return F17Constant.DefaultResponse.App.SERVER_ERROR;
        }

        return new() { AppCode = F17Constant.AppCode.SUCCESS };
    }
}