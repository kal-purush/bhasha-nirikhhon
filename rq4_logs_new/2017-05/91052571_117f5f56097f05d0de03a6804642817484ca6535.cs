using Microsoft.EntityFrameworkCore;
using SNOW.SHOP.API.Model;
using SNOW.SHOP.API.src.Model;

namespace SNOW.SHOP.API.Data
{
    public class PromotionOrderMap : IEntityMap
    {
        public void Map(ModelBuilder modelBuilder)
        {
            var entity = modelBuilder.Entity<PromotionOrder>();

            entity.ToTable("PromotionOrder", "Production");

            entity.HasKey(p => new { p.Id });

            entity.Property(p => p.Id).UseSqlServerIdentityColumn();
        }
    }
}