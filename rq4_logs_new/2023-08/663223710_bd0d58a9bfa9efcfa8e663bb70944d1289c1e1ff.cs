using LanguageExt;
using TIKSN.Data;
using static LanguageExt.Prelude;

namespace Fossa.API.Core.Repositories;

public static class RepositoryExtensions
{
  public static async Task<Option<TEntity>> GetOrNoneAsync<TEntity, TIdentity>(
    this IQueryRepository<TEntity, TIdentity> queryRepository,
    TIdentity id,
    CancellationToken cancellationToken)
      where TEntity : IEntity<TIdentity>
      where TIdentity : IEquatable<TIdentity>
  {
    var entity = await queryRepository.GetOrDefaultAsync(id, cancellationToken).ConfigureAwait(false);

    return Optional(entity);
  }
}