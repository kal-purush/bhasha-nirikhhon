using TIKSN.Data;

namespace Fossa.API.Core.Services;

public interface IBareEntityResolver<TEntity, TBareEntity, TIdentity>
  : IBareEntityResolver<TBareEntity, TIdentity>
  where TEntity : IEntity<TIdentity>
  where TBareEntity : IEntity<TIdentity>
  where TIdentity : IEquatable<TIdentity>
{
}

public interface IBareEntityResolver<TBareEntity, TIdentity>
  where TBareEntity : IEntity<TIdentity>
  where TIdentity : IEquatable<TIdentity>
{
  Task<TBareEntity> ResolveAsync(
    TIdentity id,
    CancellationToken cancellationToken);
}