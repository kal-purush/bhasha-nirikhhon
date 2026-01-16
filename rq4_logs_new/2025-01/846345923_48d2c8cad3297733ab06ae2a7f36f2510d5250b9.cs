using System;
using System.Threading;
using System.Threading.Tasks;
using F4.Src.DataAccess;
using F4.Src.Models;
using FCommon.Src.FeatureService;
using FCommon.Src.IdGeneration;

namespace F4.Src.BusinessLogic;

public sealed class F4Service : IServiceHandler<F4AppRequestModel, F4AppResponseModel>
{
    private readonly Lazy<IF4Repository> _repository;

    public F4Service(Lazy<IF4Repository> repository, Lazy<IAppIdGenerator> idGenerator)
    {
        _repository = repository;
    }

    public async Task<F4AppResponseModel> ExecuteAsync(
        F4AppRequestModel request,
        CancellationToken ct
    )
    {
        await Task.Delay(1, ct);

        return new();
    }
}