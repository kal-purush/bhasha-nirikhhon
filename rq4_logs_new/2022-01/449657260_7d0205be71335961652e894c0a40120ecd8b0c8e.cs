using cm.Domain.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace cm.Domain.Configurations
{
    public class QueryMigrationConfiguration : IEntityTypeConfiguration<QueryMigration>
    {
        public void Configure(EntityTypeBuilder<QueryMigration> builder)
        {
            builder.ToTable("_QueryMigrations");
            builder.Property(x => x.Id).HasDefaultValueSql("NEWID()");
            builder.Property(x => x.MigrationDate).HasDefaultValueSql("GETDATE()");
        }
    }
}