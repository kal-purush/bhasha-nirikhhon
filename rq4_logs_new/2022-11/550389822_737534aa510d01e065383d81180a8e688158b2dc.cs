using System;

namespace Exercism.Attribute
{
  internal enum AccountType { Guest, User, Moderator }

  [Flags]
  internal enum Permission : byte
  {
    None = 0b00000000,
    Read = 0b00000001,
    Write = 0b00000010,
    Delete = 0b00000100,
    All = 0b00000111,
  }

  static class Permissions
  {
    public static Permission Default(AccountType accountType)
    {
      return accountType switch
      {
        AccountType.Guest => Permission.Read,
        AccountType.User => Permission.Write | Permission.Read,  // 組合せを返してもいい
        AccountType.Moderator => Permission.All,
        _ => Permission.None
      };
    }

    public static Permission Grant(Permission current, Permission grant)
    {
      return current | grant;
    }

    public static Permission Revoke(Permission current, Permission revoke)
    {
      return current & ~revoke;
    }

    public static bool Check(Permission current, Permission check)
    {
      // return current.HasFlag(check);
      return (current & check) == check;
    }
  }
}
