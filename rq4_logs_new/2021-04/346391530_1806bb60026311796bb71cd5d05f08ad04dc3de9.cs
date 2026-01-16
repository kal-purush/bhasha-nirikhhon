using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using StartSpelerMVC.Models;
using System.Threading.Tasks;

namespace StartSpelerMVC.Data
{
    public class StartSpelerContext:DbContext
    {
        public StartSpelerContext(DbContextOptions<StartSpelerContext>options):base(options)
        {

        }
        public DbSet<Persoon> Personen { get; set; }
        public DbSet<Bestelling> Bestellingen { get; set; }
        public DbSet<Drankkaart> Drankkaarten { get; set; }
        public DbSet<Evenement> Evenementen { get; set; }
        public DbSet<Inschrijving> Inschrijvingen { get; set; }
        public DbSet<Orderlijn> Orderlijnen { get; set; }
        public DbSet<Product> Producten { get; set; }
        public DbSet<ProductType> productTypes { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Persoon>().ToTable("Persoon");
            modelBuilder.Entity<Bestelling>().ToTable("Bestelling");
            modelBuilder.Entity<Bestelling>().Property(p=>p.Prijs).HasColumnType("Decimal(5,2)");
            modelBuilder.Entity<Drankkaart>().ToTable("Drankkaart");
            modelBuilder.Entity<Drankkaart>().Property(p=>p.Prijs).HasColumnType("Decimal(5,2)");
            modelBuilder.Entity<Evenement>().ToTable("Evenement");
            modelBuilder.Entity<Evenement>().Property(p => p.Prijs).HasColumnType("Decimal(5,2)");
            modelBuilder.Entity<Inschrijving>().ToTable("Inschrijving");
            modelBuilder.Entity<Orderlijn>().ToTable("Orderlijn");
            modelBuilder.Entity<Product>().ToTable("Product");
            modelBuilder.Entity<Product>().Property(p=>p.VerkoopPrijs).HasColumnType("Decimal(5,2)");
            modelBuilder.Entity<Product>().Property(p=>p.AankoopPrijs).HasColumnType("Decimal(5,2)");
            modelBuilder.Entity<ProductType>().ToTable("ProductType");
        }

    }

}