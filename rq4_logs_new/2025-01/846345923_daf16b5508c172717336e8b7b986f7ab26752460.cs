using System;
using System.Threading;
using System.Threading.Tasks;
using F20.Common;
using F20.DataAccess;
using F20.Models;
using FCommon.FeatureService;

namespace F20.BusinessLogic;

public sealed class F20Service : IServiceHandler<F20AppRequestModel, F20AppResponseModel>
{
    private readonly Lazy<IF20Repository> _repository;

    public F20Service(Lazy<IF20Repository> repository)
    {
        _repository = repository;
    }

    public async Task<F20AppResponseModel> ExecuteAsync(
        F20AppRequestModel request,
        CancellationToken ct
    )
    {
        var doesTaskExist = await _repository.Value.DoesTodoTaskExistAsync(request.TodoTaskId, ct);
        if (!doesTaskExist)
        {
            return F20Constant.DefaultResponse.App.TASK_NOT_FOUND;
        }

        var isSuccess = await _repository.Value.ChangeNoteAsync(
            request.TodoTaskId,
            request.Note,
            ct
        );
        if (!isSuccess)
        {
            return F20Constant.DefaultResponse.App.SERVER_ERROR;
        }

        return new() { AppCode = F20Constant.AppCode.SUCCESS };
    }
}