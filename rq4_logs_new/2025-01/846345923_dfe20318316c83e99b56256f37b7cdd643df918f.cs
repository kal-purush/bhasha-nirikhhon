using System;
using System.Threading;
using System.Threading.Tasks;
using F13.Common;
using F13.DataAccess;
using F13.Models;
using FCommon.FeatureService;

namespace F13.BusinessLogic;

public sealed class F13Service : IServiceHandler<F13AppRequestModel, F13AppResponseModel>
{
    private readonly Lazy<IF13Repository> _repository;

    public F13Service(Lazy<IF13Repository> repository)
    {
        _repository = repository;
    }

    public async Task<F13AppResponseModel> ExecuteAsync(
        F13AppRequestModel request,
        CancellationToken ct
    )
    {
        await Task.Delay(1, ct);

        return new() { AppCode = F13Constant.AppCode.SUCCESS };
    }
}