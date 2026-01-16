using Fossa.API.Core.Tenant;
using Fossa.API.Core.User;

namespace Fossa.API.Core.Entities;

public record TenantUserEntity<TEntityIdentity, TTenantIdentity, TUserIdentity>(
  TEntityIdentity ID,
  TTenantIdentity TenantID,
  TUserIdentity UserID)
  : ITenantEntity<TEntityIdentity, TTenantIdentity>
  , IUserEntity<TEntityIdentity, TUserIdentity>
  where TEntityIdentity : IEquatable<TEntityIdentity>
  where TTenantIdentity : IEquatable<TTenantIdentity>
  where TUserIdentity : IEquatable<TUserIdentity>;