using System;
using System.Threading;
using System.Threading.Tasks;
using F9.Src.Common;
using F9.Src.DataAccess;
using F9.Src.Models;
using FCommon.Src.FeatureService;

namespace F9.Src.BusinessLogic;

public sealed class F9Service : IServiceHandler<F9AppRequestModel, F9AppResponseModel>
{
    private readonly Lazy<IF9Repository> _repository;

    public F9Service(Lazy<IF9Repository> repository)
    {
        _repository = repository;
    }

    public async Task<F9AppResponseModel> ExecuteAsync(
        F9AppRequestModel request,
        CancellationToken ct
    )
    {
        var doesListExist = await _repository.Value.DoesTaskTodoListExistAsync(
            request.TodoTaskListId,
            ct
        );
        if (!doesListExist)
        {
            return F9Constant.DefaultResponse.App.LIST_DOES_NOT_EXIST;
        }

        var todoTaskListModel = new F9TaskTodoListModel
        {
            Id = request.TodoTaskListId,
            Name = request.NewName,
        };

        var isRemoved = await _repository.Value.UpdateTaskTodoListAsync(todoTaskListModel, ct);
        if (!isRemoved)
        {
            return F9Constant.DefaultResponse.App.SERVER_ERROR;
        }

        return new() { AppCode = F9Constant.AppCode.SUCCESS };
    }
}