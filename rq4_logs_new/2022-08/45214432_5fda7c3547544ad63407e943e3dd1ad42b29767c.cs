using System.Collections.Generic;
using Microsoft.AspNetCore.Identity;

namespace MyLfc.Domain.Identity;

public class AuthUser : IdentityUser<int>// UserLogin, UserRole, UserClaim>, IEntity
{
    // public int Id { get; set; }
    // public string Email { get; set; }
    // public bool EmailConfirmed { get; set; }
    // public string PasswordHash { get; set; }
    // public string SecurityStamp { get; set; }
    // public string PhoneNumber { get; set; }
    // public bool PhoneNumberConfirmed { get; set; }
    // public bool TwoFactorEnabled { get; set; }
    // public DateTimeOffset? LockoutEnd { get; set; }
    // public bool LockoutEnabled { get; set; }
    // public int AccessFailedCount { get; set; }
    // public string UserName { get; set; }
    // public string ConcurrencyStamp { get; set; }
    // public string NormalizedEmail { get; set; }
    // public string NormalizedUserName { get; set; }

    public ICollection<IdentityUserRole<int>> Roles { get; } = new HashSet<IdentityUserRole<int>>();
    public ICollection<IdentityUserClaim<int>> Claims { get; } = new HashSet<IdentityUserClaim<int>>();
    public ICollection<IdentityUserLogin<int>> Logins { get; } = new HashSet<IdentityUserLogin<int>>();



}