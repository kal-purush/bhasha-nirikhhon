using BEBourbonCollective.Models;
using Microsoft.EntityFrameworkCore;
using BEBourbonCollective.Data;

namespace BEBourbonCollective
{
    public class BourbonCollectiveDbContext : DbContext
    {
        public DbSet<Bourbon> Bourbons { get; set; }
        public DbSet<Distillery> Distilleries { get; set; }
        public DbSet<TradeRequest> TradeRequests { get; set; }
        public DbSet<UserBourbon> UserBourbons { get; set; }
        public DbSet<User> Users { get; set; }

        public BourbonCollectiveDbContext(DbContextOptions<BourbonCollectiveDbContext> context) : base(context) { }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Bourbon>().HasData(BourbonData.Bourbons);
            modelBuilder.Entity<Distillery>().HasData(DistilleryData.Distilleries);
            modelBuilder.Entity<UserBourbon>().HasData(UserBourbonData.UserBourbons);
            modelBuilder.Entity<User>().HasData(UserData.Users);
        }
    }
}