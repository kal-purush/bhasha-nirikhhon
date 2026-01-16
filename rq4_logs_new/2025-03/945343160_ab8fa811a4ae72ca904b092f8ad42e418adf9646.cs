using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Taskly.Users.Models;

namespace Taskly.Dashboard.App.Data.Configurations
{
    public class UserConfiguration : IEntityTypeConfiguration<User>
    {
        public void Configure(EntityTypeBuilder<User> builder)
        {
            builder.ToTable("Users");

            builder.HasKey(e => e.Id);

            builder.HasIndex(e => e.PhoneNumber)
                   .IsUnique();

            builder.HasIndex(e => e.Email)
                   .IsUnique();

            builder.Property(e => e.CreatedAt)
                   .HasPrecision(0)
                   .HasDefaultValueSql("(sysdatetime())");

            builder.Property(e => e.Dob)
                   .HasColumnName("DOB");

            builder.Property(e => e.Email)
                   .HasMaxLength(255);

            builder.Property(e => e.FirstName)
                   .HasMaxLength(255);

            builder.Property(e => e.IsActive)
                   .HasDefaultValue(true);

            builder.Property(e => e.LastName)
                   .HasMaxLength(255);

            builder.Property(e => e.MiddleName)
                   .HasMaxLength(255);

            builder.Property(e => e.Password)
                   .HasMaxLength(255);

            builder.Property(e => e.PhoneNumber)
                   .HasMaxLength(20);

            builder.Property(e => e.UpdatedAt)
                   .HasPrecision(0)
                   .HasDefaultValueSql("(sysdatetime())");
        }
    }
}