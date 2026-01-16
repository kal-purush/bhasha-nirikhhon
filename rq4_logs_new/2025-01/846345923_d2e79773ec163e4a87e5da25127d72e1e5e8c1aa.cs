using System;
using System.Threading;
using System.Threading.Tasks;
using F7.Src.Common;
using F7.Src.DataAccess;
using F7.Src.Models;
using FCommon.Src.FeatureService;

namespace F7.Src.BusinessLogic;

public sealed class F7Service : IServiceHandler<F7AppRequestModel, F7AppResponseModel>
{
    private readonly Lazy<IF7Repository> _repository;

    public F7Service(Lazy<IF7Repository> repository)
    {
        _repository = repository;
    }

    public async Task<F7AppResponseModel> ExecuteAsync(
        F7AppRequestModel request,
        CancellationToken ct
    )
    {
        await Task.Delay(1, ct);

        return new() { AppCode = F7Constant.AppCode.SUCCESS };
    }
}