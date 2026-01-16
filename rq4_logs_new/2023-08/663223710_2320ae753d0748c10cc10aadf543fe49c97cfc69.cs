using TIKSN.Data;

namespace Fossa.API.Core.Messages.Commands;

public abstract record EntityTenantCommand<TEntity, TEntityIdentity, TTenantIdentity>(
  TTenantIdentity TenantID)
  : ITenantCommand<TEntityIdentity, TTenantIdentity>
  where TEntity : IEntity<TEntityIdentity>
  where TEntityIdentity : IEquatable<TEntityIdentity>
  where TTenantIdentity : IEquatable<TTenantIdentity>
{
  public abstract IEnumerable<TEntityIdentity> AffectingTenantEntitiesIdentities { get; }

  public IEnumerable<AffectingEntity<TEntityIdentity>> AffectingTenantEntities
    => AffectingTenantEntitiesIdentities.Select(x => new AffectingEntity<TEntityIdentity>(typeof(TEntity), x));
}