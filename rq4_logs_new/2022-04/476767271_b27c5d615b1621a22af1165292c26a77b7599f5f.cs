using Bcp.Domain.Models;
using Microsoft.EntityFrameworkCore;

namespace Bcp.Data
{
    public class BcpContext : DbContext
    {
        public BcpContext(DbContextOptions<BcpContext> options) 
            : base(options) 
        { }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) 
        {
            optionsBuilder.EnableSensitiveDataLogging();
        }

        public DbSet<Classification> classifications { get; set; }
        public DbSet<customer> customers { get; set; }
        public DbSet<Deliveries> deliveries { get; set; }
        public DbSet<Delnbgen> delnbgens { get; set; }
        public DbSet<Lblnbgen> lblnbgens { get; set; }
        public DbSet<OpenOrders> openorders { get; set; }   
        public DbSet<ProdMovements> prodmovements { get; set; }
        public DbSet<Routing> routing { get; set; }
        public DbSet<SapLocMarket> saplocmarket { get; set; }   
        public DbSet<Settings> settings { get; set; }
        public DbSet<StockMovements> stockmovements { get; set; }
        public DbSet<StockWms> stockwms { get; set; }
        public DbSet<Users> users { get; set; }
        public DbSet<Zp36ProdOrders> zp36prodorders { get; set; }
        public DbSet<ZtpMatDiagram> ztpmatdiagrams { get; set; }
    }
}