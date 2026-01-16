using System;
using System.Threading;
using System.Threading.Tasks;
using F15.Common;
using F15.DataAccess;
using F15.Models;
using FCommon.FeatureService;

namespace F15.BusinessLogic;

public sealed class F15Service : IServiceHandler<F15AppRequestModel, F15AppResponseModel>
{
    private readonly Lazy<IF15Repository> _repository;

    public F15Service(Lazy<IF15Repository> repository)
    {
        _repository = repository;
    }

    public async Task<F15AppResponseModel> ExecuteAsync(
        F15AppRequestModel request,
        CancellationToken ct
    )
    {
        var doesTaskExist = await _repository.Value.DoesTodoTaskExistAsync(request.TodoTaskId, ct);
        if (!doesTaskExist)
        {
            return F15Constant.DefaultResponse.App.TASK_NOT_FOUND;
        }

        var taskDetail = await _repository.Value.GetTaskDetailByIdAsync(request.TodoTaskId, ct);
        var appResponse = new F15AppResponseModel()
        {
            AppCode = F15Constant.AppCode.SUCCESS,
            Body = new()
            {
                TodoTask = new()
                {
                    Id = request.TodoTaskId,
                    Content = taskDetail.Content,
                    DueDate = taskDetail.DueDate,
                    IsFinished = taskDetail.IsFinished,
                    IsImportant = taskDetail.IsImportant,
                    IsInMyDay = taskDetail.IsInMyDay,
                    Note = taskDetail.Note,
                },
            },
        };

        return appResponse;
    }
}