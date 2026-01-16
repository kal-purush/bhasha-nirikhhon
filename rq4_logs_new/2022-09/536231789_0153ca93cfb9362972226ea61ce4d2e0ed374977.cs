using AptekaParsing.Entities;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AptekaParsing
{
    public class ApplicationContext:DbContext
    {
        public DbSet<DrugStore> Stores => Set<DrugStore>();
        public DbSet<Product> Products => Set<Product>();
        public DbSet<ProductInStore> ProductInStores => Set<ProductInStore>();
        public ApplicationContext()
        {
            Database.EnsureCreated();
        }
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseNpgsql("Host=localhost;Port=5432;Database=AptekaParsing;Username=postgres;Password=admin");
        }
    }
}