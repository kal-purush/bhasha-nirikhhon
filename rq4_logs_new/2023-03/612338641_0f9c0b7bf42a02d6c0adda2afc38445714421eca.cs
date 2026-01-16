using Core.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Infrastructure.Data.Config
{
    public class ServiceCategoryConfiguration : IEntityTypeConfiguration<ServiceCategory>
    {
        public void Configure(EntityTypeBuilder<ServiceCategory> builder)
        {
            builder
                .HasKey(x => new {
                    x.ServiceId,
                    x.CategoryId
                });

            builder
                .HasOne(x => x.Category)
                .WithMany(x => x.ServiceCategories)
                .HasForeignKey(x => x.CategoryId)
                .OnDelete(DeleteBehavior.NoAction);

            builder
                .HasOne(x => x.Service)
                .WithOne(x => x.ServiceCategory)
                .OnDelete(DeleteBehavior.Cascade);

            builder.Property(x => x.ServiceId).IsRequired();
            builder.Property(x => x.CategoryId).IsRequired();

            builder.Navigation(x => x.Service).AutoInclude();
            builder.Navigation(x => x.Category).AutoInclude();
        }
    }
}