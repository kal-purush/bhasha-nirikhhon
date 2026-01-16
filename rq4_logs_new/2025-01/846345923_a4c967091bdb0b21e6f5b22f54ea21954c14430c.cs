using System;
using System.Threading;
using System.Threading.Tasks;
using F8.Src.Common;
using F8.Src.DataAccess;
using F8.Src.Models;
using FCommon.Src.FeatureService;

namespace F8.Src.BusinessLogic;

public sealed class F8Service : IServiceHandler<F8AppRequestModel, F8AppResponseModel>
{
    private readonly Lazy<IF8Repository> _repository;

    public F8Service(Lazy<IF8Repository> repository)
    {
        _repository = repository;
    }

    public async Task<F8AppResponseModel> ExecuteAsync(
        F8AppRequestModel request,
        CancellationToken ct
    )
    {
        await Task.Delay(1, ct);

        return new() { AppCode = F8Constant.AppCode.SUCCESS };
    }
}