using System;
using System.Threading;
using System.Threading.Tasks;
using F19.Common;
using F19.DataAccess;
using F19.Models;
using FCommon.FeatureService;

namespace F19.BusinessLogic;

public sealed class F19Service : IServiceHandler<F19AppRequestModel, F19AppResponseModel>
{
    private readonly Lazy<IF19Repository> _repository;

    public F19Service(Lazy<IF19Repository> repository)
    {
        _repository = repository;
    }

    public async Task<F19AppResponseModel> ExecuteAsync(
        F19AppRequestModel request,
        CancellationToken ct
    )
    {
        var doesTaskExist = await _repository.Value.DoesTodoTaskExistAsync(request.TodoTaskId, ct);
        if (!doesTaskExist)
        {
            return F19Constant.DefaultResponse.App.TASK_NOT_FOUND;
        }

        var isSuccess = await _repository.Value.ChangeContentAsync(
            request.TodoTaskId,
            request.Content,
            ct
        );
        if (!isSuccess)
        {
            return F19Constant.DefaultResponse.App.SERVER_ERROR;
        }

        return new() { AppCode = F19Constant.AppCode.SUCCESS };
    }
}