using System;
using System.Threading;
using System.Threading.Tasks;
using F21.Common;
using F21.DataAccess;
using F21.Models;
using FCommon.FeatureService;

namespace F21.BusinessLogic;

public sealed class F21Service : IServiceHandler<F21AppRequestModel, F21AppResponseModel>
{
    private readonly Lazy<IF21Repository> _repository;

    public F21Service(Lazy<IF21Repository> repository)
    {
        _repository = repository;
    }

    public async Task<F21AppResponseModel> ExecuteAsync(
        F21AppRequestModel request,
        CancellationToken ct
    )
    {
        var doesTaskExist = await _repository.Value.DoesTodoTaskExistAsync(request.TodoTaskId, ct);
        if (!doesTaskExist)
        {
            return F21Constant.DefaultResponse.App.TASK_NOT_FOUND;
        }

        var isSuccess = await _repository.Value.ChangeDueDateAsync(
            request.TodoTaskId,
            request.DueDate,
            ct
        );
        if (!isSuccess)
        {
            return F21Constant.DefaultResponse.App.SERVER_ERROR;
        }

        return new() { AppCode = F21Constant.AppCode.SUCCESS };
    }
}