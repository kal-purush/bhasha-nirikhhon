using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using SelfService.Domain.Models;

namespace SelfService.Infrastructure.Persistence.Converters;

public class MembershipApplicationStatusOptionsConverter : ValueConverter<MembershipApplicationStatusOptions, string>
{
    public MembershipApplicationStatusOptionsConverter() : base(ToDatabaseType, FromDatabaseType)
    {

    }

    private static Expression<Func<MembershipApplicationStatusOptions, string>> ToDatabaseType => id => id.ToString();
    private static Expression<Func<string, MembershipApplicationStatusOptions>> FromDatabaseType => value => MembershipApplicationStatusOptions.Parse(value);
}