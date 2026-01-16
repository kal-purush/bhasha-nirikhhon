using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Domain;

namespace Persistence.Configuration
{
    public class FridgeModelConfiguration : IEntityTypeConfiguration<FridgeModel>
    {
        public void Configure(EntityTypeBuilder<FridgeModel> builder)
        {
            builder.HasData(
                new FridgeModel
                {
                    Id = Guid.Parse("3BFD5E1C-8407-4D3E-B77B-30427468656E"),
                    Name = "Samsung RB34A7B4F35/WT",
                    Year = 2020
                },
                new FridgeModel
                {
                    Id = Guid.Parse("D957B9BE-B351-4629-A332-4841851AA395"),
                    Name = "LG DoorCooling+ GA-B509CQTL",
                    Year = 2020
                },
                new FridgeModel
                {
                    Id = Guid.Parse("B12741D6-4EA0-4CE4-8A4E-3BE45767B928"),
                    Name = "Electrolux KNT2LF18S",
                    Year = 2019
                },
                new FridgeModel
                {
                    Id = Guid.Parse("B0463F44-AF0C-434F-9667-C3BF6C9F8A93"),
                    Name = "ATLANT ХМ 4307-000",
                    Year = 2017
                },
                new FridgeModel
                {
                    Id = Guid.Parse("2767F531-6EAB-492B-99FA-839B826552E9"),
                    Name = "Indesit ITR 5200 W",
                    Year = 2020
                }
            );
        }
    }
}