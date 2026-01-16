using Fossa.API.Core.Messages.Commands;
using Fossa.API.Core.Messages.Queries;
using Fossa.API.Core.Tenant;
using MediatR;

namespace Fossa.API.Core.Messages;

public class TenantRequestBehavior<TRequest, TResponse> : TenantRequestBehavior<long, Guid, TRequest, TResponse> where TRequest : IRequest<TResponse>
{
  public TenantRequestBehavior(ITenantIdProvider<Guid> tenantIdProvider) : base(tenantIdProvider)
  {
  }
}

public class TenantRequestBehavior<TEntityIdentity, TTenantIdentity, TRequest, TResponse>
  : IPipelineBehavior<TRequest, TResponse>
  where TRequest : IRequest<TResponse>
  where TEntityIdentity : IEquatable<TEntityIdentity>
  where TTenantIdentity : IEquatable<TTenantIdentity>
{
  private readonly ITenantIdProvider<TTenantIdentity> _tenantIdProvider;

  public TenantRequestBehavior(ITenantIdProvider<TTenantIdentity> tenantIdProvider)
  {
    _tenantIdProvider = tenantIdProvider ?? throw new ArgumentNullException(nameof(tenantIdProvider));
  }

  public async Task<TResponse> Handle(
    TRequest request,
    RequestHandlerDelegate<TResponse> next,
    CancellationToken cancellationToken)
  {
    await InspectRequestAsync(request, cancellationToken).ConfigureAwait(false);

    var response = await next().ConfigureAwait(false);

    await InspectResponseAsync(response, cancellationToken).ConfigureAwait(false);

    return response;
  }

  private Task InspectRequestAsync(
    TRequest request,
    CancellationToken cancellationToken)
  {
    if (request is ITenantCommand<TTenantIdentity> tenantCommand)
    {
      return InspectTenantCommandAsync(tenantCommand, cancellationToken);
    }

    if (request is ITenantQuery<TTenantIdentity, TResponse> tenantQuery)
    {
      return InspectTenantQueryAsync(tenantQuery, cancellationToken);
    }

    throw new InvalidOperationException("Request is not Tenant Command or Tenant Query");
  }

  private async Task InspectResponseAsync(
    TResponse? response,
    CancellationToken cancellationToken)
  {
    if (response is not null && response is not Unit)
    {
      var tenantId = _tenantIdProvider.GetTenantId();

      if (response is ITenantEntity<TEntityIdentity, TTenantIdentity> tenantEntity)
      {
        await InspectTenantEntityAsync(tenantId, tenantEntity, cancellationToken).ConfigureAwait(false);
      }
      else if (response is IEnumerable<ITenantEntity<TEntityIdentity, TTenantIdentity>> tenantEntities)
      {
        foreach (var currentTenantEntity in tenantEntities)
        {
          await InspectTenantEntityAsync(tenantId, currentTenantEntity, cancellationToken).ConfigureAwait(false);
        }
      }
      else
      {
        throw new InvalidOperationException($"Unknown response type {response?.GetType()?.FullName}");
      }
    }
  }

  private Task InspectTenantCommandAsync(
    ITenantCommand<TTenantIdentity> tenantCommand,
    CancellationToken cancellationToken)
  {
    var tenantId = _tenantIdProvider.GetTenantId();

    if (tenantCommand.TenantID == null || !tenantCommand.TenantID.Equals(tenantId))
    {
      throw new CrossTenantInboundUnauthorizedAccessException();
    }

    cancellationToken.ThrowIfCancellationRequested();

    return Task.CompletedTask;
  }

  private Task InspectTenantEntityAsync(
    TTenantIdentity tenantId,
    ITenantEntity<TEntityIdentity, TTenantIdentity> tenantEntity,
    CancellationToken cancellationToken)
  {
    if (tenantEntity.TenantID == null || !tenantEntity.TenantID.Equals(tenantId))
    {
      throw new CrossTenantOutboundUnauthorizedAccessException();
    }

    cancellationToken.ThrowIfCancellationRequested();

    return Task.CompletedTask;
  }

  private Task InspectTenantQueryAsync(
    ITenantQuery<TTenantIdentity, TResponse> tenantQuery,
    CancellationToken cancellationToken)
  {
    var tenantId = _tenantIdProvider.GetTenantId();

    if (tenantQuery.TenantID == null || !tenantQuery.TenantID.Equals(tenantId))
    {
      throw new CrossTenantInboundUnauthorizedAccessException();
    }

    cancellationToken.ThrowIfCancellationRequested();

    return Task.CompletedTask;
  }
}