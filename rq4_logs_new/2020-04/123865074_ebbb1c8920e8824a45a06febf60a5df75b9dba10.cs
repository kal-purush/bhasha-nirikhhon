using Com.Danliris.Service.Inventory.Lib.Models.GarmentLeftoverWarehouse.GarmentLeftoverWarehouseReceiptAvalModels;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using System;
using System.Collections.Generic;
using System.Text;

namespace Com.Danliris.Service.Inventory.Lib.Configs.GarmentLeftoverWarehouse.GarmentLeftoverWarehouseReceiptAvalConfigs
{
    public class GarmentLeftoverWarehouseReceiptAvalConfig : IEntityTypeConfiguration<GarmentLeftoverWarehouseReceiptAval>
    {
        public void Configure(EntityTypeBuilder<GarmentLeftoverWarehouseReceiptAval> builder)
        {
            builder.Property(p => p.AvalReceiptNo)
                .HasMaxLength(25)
                .IsRequired();

            builder.HasIndex(i => i.AvalReceiptNo)
                .IsUnique()
                .HasFilter("[_IsDeleted]=(0)");

            builder.Property(p => p.UnitFromCode).HasMaxLength(25);
            builder.Property(p => p.UnitFromName).HasMaxLength(100);

            builder.Property(p => p.AvalType).HasMaxLength(25);
            builder.Property(p => p.Remark).HasMaxLength(3000);

            builder
                .HasMany(h => h.Items)
                .WithOne(w => w.GarmentLeftoverWarehouseReceiptAval)
                .HasForeignKey(f => f.AvalReceiptId)
                .IsRequired();
        }
    }
}