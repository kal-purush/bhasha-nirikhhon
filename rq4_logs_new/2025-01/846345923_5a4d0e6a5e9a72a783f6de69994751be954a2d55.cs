using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using F14.Common;
using F14.DataAccess;
using F14.Models;
using FCommon.FeatureService;

namespace F14.BusinessLogic;

public sealed class F14Service : IServiceHandler<F14AppRequestModel, F14AppResponseModel>
{
    private readonly Lazy<IF14Repository> _repository;

    public F14Service(Lazy<IF14Repository> repository)
    {
        _repository = repository;
    }

    public async Task<F14AppResponseModel> ExecuteAsync(
        F14AppRequestModel request,
        CancellationToken ct
    )
    {
        var doesListExist = await _repository.Value.DoesTodoTaskListExistAsync(
            request.TodoTaskListId,
            ct
        );
        if (!doesListExist)
        {
            return F14Constant.DefaultResponse.App.TODO_TASK_LIST_NOT_FOUND;
        }

        var doesTaskExist = await _repository.Value.DoesTodoTaskExistAsync(
            request.TodoTaskId,
            request.TodoTaskListId,
            ct
        );
        if (!doesTaskExist && request.TodoTaskId != 0)
        {
            return F14Constant.DefaultResponse.App.TASK_NOT_FOUND;
        }

        var input = new F14GetTodoTasksInputModel
        {
            TodoTaskId = request.TodoTaskId,
            TodoTaskListId = request.TodoTaskListId,
            NumberOfRecord = request.NumberOfRecord,
        };
        var foundTodoTasks = await _repository.Value.GetTodoTasksAsync(input, ct);

        var taskCount = foundTodoTasks.Count();
        if (taskCount <= request.NumberOfRecord)
        {
            return new()
            {
                AppCode = F14Constant.AppCode.SUCCESS,
                Body = new()
                {
                    TodoTasks = foundTodoTasks.Select(
                        model => new F14AppResponseModel.BodyModel.TodoTaskModel
                        {
                            Id = model.Id,
                            Content = model.Content,
                            DueDate = model.DueDate,
                            IsImportant = model.IsImportant,
                            IsInMyDay = model.IsInMyDay,
                            HasNote = model.HasNote,
                            HasSteps = model.HasSteps,
                            IsRecurring = model.IsRecurring,
                        }
                    ),
                    NextCursor = 0,
                },
            };
        }

        var appResponseTodoTasks = new List<F14AppResponseModel.BodyModel.TodoTaskModel>();
        for (var i = 0; i < taskCount; i++)
        {
            var taskDetail = foundTodoTasks.ElementAt(i);

            appResponseTodoTasks.Add(
                new()
                {
                    Id = taskDetail.Id,
                    Content = taskDetail.Content,
                    DueDate = taskDetail.DueDate,
                    IsImportant = taskDetail.IsImportant,
                    IsInMyDay = taskDetail.IsInMyDay,
                    HasNote = taskDetail.HasNote,
                    HasSteps = taskDetail.HasSteps,
                    IsRecurring = taskDetail.IsRecurring,
                }
            );
        }
        var nextCursor = appResponseTodoTasks[^1].Id;
        appResponseTodoTasks.RemoveAt(appResponseTodoTasks.Count - 1);

        return new()
        {
            AppCode = F14Constant.AppCode.SUCCESS,
            Body = new() { TodoTasks = appResponseTodoTasks, NextCursor = nextCursor },
        };
    }
}