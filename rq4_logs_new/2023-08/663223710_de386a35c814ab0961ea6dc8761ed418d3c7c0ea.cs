using TIKSN.Data;

namespace Fossa.API.Core.Messages.Queries;

public abstract record EntityTenantQuery<TEntity, TEntityIdentity, TTenantIdentity, TResult>(
  TTenantIdentity TenantID)
  : ITenantQuery<TEntityIdentity, TTenantIdentity, TResult>
  where TEntity : IEntity<TEntityIdentity>
  where TEntityIdentity : IEquatable<TEntityIdentity>
  where TTenantIdentity : IEquatable<TTenantIdentity>
{
  public abstract IEnumerable<TEntityIdentity> AffectingTenantEntitiesIdentities { get; }

  public IEnumerable<AffectingEntity<TEntityIdentity>> AffectingTenantEntities
    => AffectingTenantEntitiesIdentities.Select(x => new AffectingEntity<TEntityIdentity>(typeof(TEntity), x));
}