using System;
using System.Threading;
using System.Threading.Tasks;
using F12.Common;
using F12.DataAccess;
using F12.Models;
using FCommon.FeatureService;

namespace F12.BusinessLogic;

public sealed class F12Service : IServiceHandler<F12AppRequestModel, F12AppResponseModel>
{
    private readonly Lazy<IF12Repository> _repository;

    public F12Service(Lazy<IF12Repository> repository)
    {
        _repository = repository;
    }

    public async Task<F12AppResponseModel> ExecuteAsync(
        F12AppRequestModel request,
        CancellationToken ct
    )
    {
        await Task.CompletedTask;

        return new() { AppCode = F12Constant.AppCode.SUCCESS };
    }
}