using Domain;
using Microsoft.EntityFrameworkCore;
using Persistence;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UnitTests.MoqObjects
{
    public class FridgeDbContextFactory
    {
        public static FridgeDbContext Create()
        {
            var options = new DbContextOptionsBuilder<FridgeDbContext>()
               .UseInMemoryDatabase(Guid.NewGuid().ToString())
               .Options;
            var context = new FridgeDbContext(options);
            context.Database.EnsureCreated();
            context.FridgeModels.AddRange(
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
            context.Fridges.AddRange(
                    new Fridge
                    {
                        Id = Guid.Parse("7870E84D-0F97-4196-BED7-19BD8FF40A63"),
                        Name = "Electrolux Dima",
                        OwnerName = "Dima",
                        FridgeModelId = Guid.Parse("B12741D6-4EA0-4CE4-8A4E-3BE45767B928")
                    },
                    new Fridge
                    {
                        Id = Guid.Parse("B97C170D-A6BF-4F26-8231-28D9025BF3AD"),
                        Name = "ATLANT Vlad",
                        OwnerName = "Vlad",
                        FridgeModelId = Guid.Parse("B0463F44-AF0C-434F-9667-C3BF6C9F8A93")
                    },
                    new Fridge
                    {
                        Id = Guid.Parse("557D35EF-AB80-4E17-A96C-2B65CF3DD7BF"),
                        Name = "Electrolux Sasha",
                        OwnerName = "Sasha",
                        FridgeModelId = Guid.Parse("B12741D6-4EA0-4CE4-8A4E-3BE45767B928")
                    }
                );

            context.SaveChanges();
            return context;            
        }
        public static void Destroy(FridgeDbContext context)
        {
            context.Database.EnsureCreated();
            context.Dispose();
        }
    }
}