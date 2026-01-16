using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore;
using Mod4.Lection4.Hw1.Models;

namespace Mod4.Lection4.Hw1.Context.Configurations;

public class AddressConfiguration
{
    public void Configure(EntityTypeBuilder<Address> builder)
    {
        builder.HasKey(x => x.Id);
        builder.Property(b => b.Street).IsRequired();
        builder.Property(b => b.City).IsRequired();

        // 1-1
        builder
            .HasOne(x => x.User)
            .WithOne(x => x.UserAddress)
            .OnDelete(DeleteBehavior.ClientSetNull);
    }
}