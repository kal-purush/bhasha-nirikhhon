using Microsoft.EntityFrameworkCore;
using ReptileTracker.Animal.Model;
using ReptileTracker.Feeding.Model;
using ReptileTracker.Shedding.Model;

namespace ReptileTracker.Db;

public class ReptileContext(IConfiguration configuration) : DbContext
{
    public DbSet<Account.Model.Account>? Accounts { get; set; }
    public DbSet<Length>? Lengths { get; set; }
    public DbSet<Reptile>? Reptiles { get; set; }
    public DbSet<Weight>? Weights { get; set; }
    public DbSet<FeedingEvent>? FeedingEvents { get; set; }
    public DbSet<SheddingEvent>? SheddingEvents { get; set; }
    
    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseSqlServer(configuration.GetConnectionString("reptileDb"), x
            => x.MigrationsHistoryTable("__EFMigrationsHistory", "dbo"));

}