using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Mod4.Lection4.Hw.Models;

namespace Mod4.Lection4.Hw.Context.Configurations;

public class UserConfiguration : IEntityTypeConfiguration<User>
{
    public void Configure(EntityTypeBuilder<User> builder)
    {
        builder.HasKey(x => x.Id);
        builder.Property(b => b.Name).IsRequired();
        builder.Property(b => b.Surname).IsRequired();

        builder
            .Property(x => x.Birthday)
            .HasConversion(new ValueConverter<DateOnly?, DateTime?>(
                x => x.HasValue ? x.Value.ToDateTime(TimeOnly.MinValue) : null,
                y => y.HasValue ? DateOnly.FromDateTime(y.Value) : null)
            );

        builder
            .HasOne(x => x.UserSettings)
            .WithOne(x => x.User)
            .OnDelete(DeleteBehavior.ClientSetNull);

        builder.HasMany(x => x.Articles).WithMany(x => x.Athors);
        builder.HasMany(x => x.BlogSubscribsions).WithMany(x => x.Readers);
    }
}