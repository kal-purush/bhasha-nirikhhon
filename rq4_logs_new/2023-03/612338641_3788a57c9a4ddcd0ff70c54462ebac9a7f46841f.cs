using Core.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Infrastructure.Data.Config
{
    public class ItemClassConfiguration : IEntityTypeConfiguration<ItemClass>
    {
        public void Configure(EntityTypeBuilder<ItemClass> builder)
        {
            builder
                .HasMany(x => x.ProductItemClasses)
                .WithOne(x => x.ItemClass)
                .HasForeignKey(x => x.ItemClassId)
                .IsRequired()
                .OnDelete(DeleteBehavior.Restrict);
        }
    }
}