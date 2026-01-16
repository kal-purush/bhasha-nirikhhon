namespace Fossa.API.Core.Messages.Events;

public interface ITenantUserEvent<out TUserIdentity, out TTenantIdentity>
  : ITenantEvent<TTenantIdentity>
  , IUserEvent<TUserIdentity>
  where TTenantIdentity : IEquatable<TTenantIdentity>
  where TUserIdentity : IEquatable<TUserIdentity>
{
}