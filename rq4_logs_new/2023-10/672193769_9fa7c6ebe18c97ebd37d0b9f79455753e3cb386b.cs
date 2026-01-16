using api.Users.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace api.Data.ModelsConfigurations;

public class UserConfiguration : IEntityTypeConfiguration<User>
{
    public void Configure(EntityTypeBuilder<User> builder)
    {
        builder.Property(x => x.Email).HasColumnType("varchar").HasMaxLength(255);
        builder.Property(x => x.FirstName).HasColumnType("varchar").HasMaxLength(255);
        builder.Property(x => x.LastName).HasColumnType("varchar").HasMaxLength(255);
        builder.Property(x => x.Password).HasColumnType("varchar").HasMaxLength(255);
        builder.Property(x => x.ApiKey).HasColumnType("varchar").HasMaxLength(500);
    }
}