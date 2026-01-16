using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using F10.Src.Common;
using F10.Src.DataAccess;
using F10.Src.Models;
using FCommon.Src.FeatureService;

namespace F10.Src.BusinessLogic;

public sealed class F10Service : IServiceHandler<F10AppRequestModel, F10AppResponseModel>
{
    private readonly Lazy<IF10Repository> _repository;

    public F10Service(Lazy<IF10Repository> repository)
    {
        _repository = repository;
    }

    public async Task<F10AppResponseModel> ExecuteAsync(
        F10AppRequestModel request,
        CancellationToken ct
    )
    {
        var foundTodoTaskLists = await _repository.Value.GetTodoTaskListAsync(
            request.TodoTaskListId,
            request.NumberOfRecord,
            ct
        );

        var listCount = foundTodoTaskLists.Count();
        if (listCount == 0)
        {
            return new()
            {
                AppCode = F10Constant.AppCode.SUCCESS,
                Body = new() { TodoTaskLists = [], NextCursor = 0 },
            };
        }

        if (listCount <= request.NumberOfRecord)
        {
            return new()
            {
                AppCode = F10Constant.AppCode.SUCCESS,
                Body = new()
                {
                    TodoTaskLists = foundTodoTaskLists.Select(
                        model => new F10AppResponseModel.BodyModel.TodoTaskListModel
                        {
                            Id = model.Id,
                            Name = model.Name,
                        }
                    ),
                    NextCursor = 0,
                },
            };
        }

        var appResponseTodoTaskLists = new List<F10AppResponseModel.BodyModel.TodoTaskListModel>();
        for (var i = 0; i < listCount; i++)
        {
            var element = foundTodoTaskLists.ElementAt(i);

            appResponseTodoTaskLists.Add(new() { Id = element.Id, Name = element.Name });
        }
        var nextCursor = appResponseTodoTaskLists[^1].Id;
        appResponseTodoTaskLists.RemoveAt(appResponseTodoTaskLists.Count - 1);

        return new()
        {
            AppCode = F10Constant.AppCode.SUCCESS,
            Body = new() { TodoTaskLists = appResponseTodoTaskLists, NextCursor = nextCursor },
        };
    }
}