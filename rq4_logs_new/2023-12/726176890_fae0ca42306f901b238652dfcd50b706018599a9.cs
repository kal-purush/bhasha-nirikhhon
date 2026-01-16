using MagicVilla.Models.DTO;
using Microsoft.EntityFrameworkCore;

namespace MagicVilla.Data
{
    public class ApplicationDbContext : DbContext
    {
        public ApplicationDbContext(DbContextOptions options)
            : base(options)
        {
            
        }

        public DbSet<VillaDTO> Villas { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<VillaDTO>().HasData(
                new VillaDTO { Id = 1, Name = "ALAYA BARBADOS" },
                new VillaDTO { Id = 2, Name = "VILLA RICA IBIZA" },
                new VillaDTO { Id = 3, Name = "CASA TRES SOLES" },
                new VillaDTO { Id = 4, Name = "LA BERGERIE" }
                );
        }
    }
}