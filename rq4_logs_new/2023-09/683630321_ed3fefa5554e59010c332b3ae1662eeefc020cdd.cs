using Common.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Infrastructure.Data.ApplicationDbContext.Mappings
{
    public class UserRoleLookupMapping : IEntityTypeConfiguration<UserRoleLookup>
    {
        public void Configure(EntityTypeBuilder<UserRoleLookup> builder)
        {
            builder.HasKey(c=> c.RoleId);
            builder
                .Property(c => c.RoleId)
                .ValueGeneratedOnAdd();
            builder
                .HasMany(c => c.ApplicationUsers)
                .WithOne(c => c.Role)
                .HasForeignKey(c => c.RoleId);
        }
    }
}