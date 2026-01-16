using GiftSuggester.Data.Gifts.Models;
using GiftSuggester.Data.Groups.Models;
using GiftSuggester.Data.Users.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace GiftSuggester.Data;

public class GiftSuggesterContext : DbContext
{
    public DbSet<GiftDbModel> Gifts { get; set; } = null!;
    public DbSet<GroupDbModel> Groups { get; set; } = null!;
    public DbSet<UserDbModel> Users { get; set; } = null!;

    public GiftSuggesterContext(DbContextOptions options) : base(options)
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfigurationsFromAssembly(typeof(GiftSuggesterContext).Assembly);
        base.OnModelCreating(modelBuilder);
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder
            .UseSnakeCaseNamingConvention()
            .UseLazyLoadingProxies();
    }

    public class Factory : IDesignTimeDbContextFactory<GiftSuggesterContext>
    {
        public GiftSuggesterContext CreateDbContext(string[] args)
        {
            var options = new DbContextOptionsBuilder()
                .UseNpgsql("FakeConnectionStringOnlyForMigrations")
                .Options;

            return new GiftSuggesterContext(options);
        }
    }
}