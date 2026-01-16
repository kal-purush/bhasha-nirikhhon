using Microsoft.EntityFrameworkCore;
using RMS.Domain.Abstract;
using RMS.Infrastructure.Persistence.TablesConfiguration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RMS.Infrastructure.Persistence.DataBases
{
    public class EFContext : DbContext
    {
        public EFContext(DbContextOptions<EFContext> options) : base(options)
        {

        }
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseNpgsql("Host=localhost;Port=5432;Database=RMS;Username=postgres;Password=7878_data_base");
            base.OnConfiguring(optionsBuilder);
        }
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            modelBuilder.Ignore<EntityBase>();
            modelBuilder.ApplyConfigurationsFromAssembly(typeof(TablesConfiguration.CustomerConfiguration).Assembly);
            modelBuilder.ApplyConfigurationsFromAssembly(typeof(TablesConfiguration.MenuItemConfiguration).Assembly);
            modelBuilder.ApplyConfigurationsFromAssembly(typeof(TablesConfiguration.OrderItemConfiguration).Assembly);
            modelBuilder.ApplyConfigurationsFromAssembly(typeof(TablesConfiguration.OrderItemConfiguration).Assembly);
            modelBuilder.ApplyConfigurationsFromAssembly(typeof(TablesConfiguration.PaymentConfiguration).Assembly);
            modelBuilder.ApplyConfigurationsFromAssembly(typeof(TablesConfiguration.ReservationConfiguration).Assembly);
            modelBuilder.ApplyConfigurationsFromAssembly(typeof(TablesConfiguration.TableConfiguration).Assembly);
        }
    }
}