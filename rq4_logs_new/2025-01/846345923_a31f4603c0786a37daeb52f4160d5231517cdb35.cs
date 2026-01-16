using System;
using System.Threading;
using System.Threading.Tasks;
using F11.DataAccess;
using F11.Models;
using FCommon.FeatureService;

namespace F11.BusinessLogic;

public sealed class F11Service : IServiceHandler<F11AppRequestModel, F11AppResponseModel>
{
    private readonly Lazy<IF11Repository> _repository;

    public F11Service(Lazy<IF11Repository> repository)
    {
        _repository = repository;
    }

    public async Task<F11AppResponseModel> ExecuteAsync(
        F11AppRequestModel request,
        CancellationToken ct
    )
    {
        await Task.CompletedTask;

        return new();
    }
}