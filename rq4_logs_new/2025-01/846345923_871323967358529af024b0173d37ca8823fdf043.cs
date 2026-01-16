using System;
using System.Threading;
using System.Threading.Tasks;
using F18.Common;
using F18.DataAccess;
using F18.Models;
using FCommon.FeatureService;

namespace F18.BusinessLogic;

public sealed class F18Service : IServiceHandler<F18AppRequestModel, F18AppResponseModel>
{
    private readonly Lazy<IF18Repository> _repository;

    public F18Service(Lazy<IF18Repository> repository)
    {
        _repository = repository;
    }

    public async Task<F18AppResponseModel> ExecuteAsync(
        F18AppRequestModel request,
        CancellationToken ct
    )
    {
        var doesTaskExist = await _repository.Value.DoesTodoTaskExistAsync(request.TodoTaskId, ct);
        if (!doesTaskExist)
        {
            return F18Constant.DefaultResponse.App.TASK_NOT_FOUND;
        }

        var isSuccess = await _repository.Value.ModifyIsImportantStatusAsync(
            request.TodoTaskId,
            request.IsImportant,
            ct
        );
        if (!isSuccess)
        {
            return F18Constant.DefaultResponse.App.SERVER_ERROR;
        }

        return new() { AppCode = F18Constant.AppCode.SUCCESS };
    }
}