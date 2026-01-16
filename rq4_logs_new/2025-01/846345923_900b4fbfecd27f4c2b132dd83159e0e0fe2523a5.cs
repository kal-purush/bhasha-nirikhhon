using System;
using System.Threading;
using System.Threading.Tasks;
using F6.Src.Common;
using F6.Src.DataAccess;
using F6.Src.Models;
using FCommon.Src.FeatureService;

namespace F6.Src.BusinessLogic;

public sealed class F6Service : IServiceHandler<F6AppRequestModel, F6AppResponseModel>
{
    private readonly Lazy<IF6Repository> _repository;

    public F6Service(Lazy<IF6Repository> repository)
    {
        _repository = repository;
    }

    public async Task<F6AppResponseModel> ExecuteAsync(
        F6AppRequestModel request,
        CancellationToken ct
    )
    {
        await Task.Delay(1, ct);

        return new() { AppCode = F6Constant.AppCode.SUCCESS };
    }
}