using Com.Danliris.Service.Inventory.Lib.Models.GarmentLeftoverWarehouse.Stock;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Com.Danliris.Service.Inventory.Lib.Configs.GarmentLeftoverWarehouse.Stock
{
    public class GarmentLeftoverWarehouseStockConfig : IEntityTypeConfiguration<GarmentLeftoverWarehouseStock>
    {
        public void Configure(EntityTypeBuilder<GarmentLeftoverWarehouseStock> builder)
        {
            builder.Property(p => p.UnitCode).HasMaxLength(25);
            builder.Property(p => p.UnitName).HasMaxLength(100);
            builder.Property(p => p.PONo).HasMaxLength(25);
            builder.Property(p => p.RONo).HasMaxLength(25);
            builder.Property(p => p.ProductCode).HasMaxLength(25);
            builder.Property(p => p.ProductName).HasMaxLength(100);
            builder.Property(p => p.UomUnit).HasMaxLength(100);

            builder.HasIndex(p => new { p.ReferenceType, p.UnitId, p.PONo, p.RONo, p.KG, p.ProductId, p.UomId })
                .IsUnique()
                .HasFilter("[_IsDeleted]=(0)");

            builder.Property(p => p.ReferenceType)
                .HasMaxLength(25)
                .HasConversion<string>();

            builder
                .HasMany(h => h.Histories)
                .WithOne(w => w.Stock)
                .HasForeignKey(f => f.StockId)
                .IsRequired();
        }
    }
}