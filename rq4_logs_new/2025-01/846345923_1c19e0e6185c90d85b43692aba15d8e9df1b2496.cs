using System;
using System.Threading;
using System.Threading.Tasks;
using F5.Src.DataAccess;
using F5.Src.Models;
using FCommon.Src.FeatureService;
using FCommon.Src.IdGeneration;

namespace F5.Src.BusinessLogic;

public sealed class F5Service : IServiceHandler<F5AppRequestModel, F5AppResponseModel>
{
    private readonly Lazy<IF5Repository> _repository;

    public F5Service(Lazy<IF5Repository> repository, Lazy<IAppIdGenerator> idGenerator)
    {
        _repository = repository;
    }

    public async Task<F5AppResponseModel> ExecuteAsync(
        F5AppRequestModel request,
        CancellationToken ct
    )
    {
        await Task.Delay(1, ct);

        return null;
    }
}