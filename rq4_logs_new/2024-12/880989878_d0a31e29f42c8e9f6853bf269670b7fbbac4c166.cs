using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore;
using MarGate.Basket.Domain.Entities;

namespace MarGate.Basket.Persistence.Configurations;

public class BasketItemConfiguration : IEntityTypeConfiguration<BasketItem>
{
    public void Configure(EntityTypeBuilder<BasketItem> builder)
    {
        builder.HasKey(bi => bi.Id);
        builder.Property(bi => bi.Id).ValueGeneratedOnAdd();

        builder.Property(bi => bi.CreatedDate)
               .IsRequired()
               .HasDefaultValueSql("GETDATE()");

        builder.Property(bi => bi.ModifiedDate)
               .IsRequired()
               .ValueGeneratedOnAddOrUpdate()
               .HasDefaultValueSql("GETDATE()");

        builder.Property(bi => bi.IsDeleted).IsRequired();

        builder.HasOne<Product>(bi => bi.Product)
               .WithMany(p => p.BasketItems)
               .HasForeignKey(bi => bi.ProductId);

        builder.HasOne<Basket>(bi => bi.Basket)
               .WithMany(b => b.BasketItems)
               .HasForeignKey(bi => bi.BasketId);

        builder.ToTable("BasketItems");
    }
}