using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using PocketForzaHorizonCommunity.Back.Database.Entities;
using PocketForzaHorizonCommunity.Back.Database.Entities.CarEntities;
using PocketForzaHorizonCommunity.Back.Database.Entities.Guides;
using PocketForzaHorizonCommunity.Back.Database.Entities.UserStatistics;

namespace PocketForzaHorizonCommunity.Back.Database;

public class ApplicationDbContext : IdentityDbContext<ApplicationUser, IdentityRole<Guid>, Guid>, IApplicationDbContext
{
    public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options) : base(options) { }

    public DbSet<Car> Cars => Set<Car>();
    public DbSet<Manufacture> Manufactures => Set<Manufacture>();
    public DbSet<CarType> Types => Set<CarType>();
    public DbSet<Design> Designs => Set<Design>();
    public DbSet<DesignOptions> DesignsOptions => Set<DesignOptions>();
    public DbSet<Tune> Tunes => Set<Tune>();
    public DbSet<TuneOptions> TunesOptions => Set<TuneOptions>();
    public DbSet<CampaignStatistics> CampaignStatistics => Set<CampaignStatistics>();
    public DbSet<GeneralStatistics> GeneralStatistics => Set<GeneralStatistics>();
    public DbSet<OnlineStatistics> OnlineStatistics => Set<OnlineStatistics>();
    public DbSet<RecordStatistics> RecordStatistics => Set<RecordStatistics>();
}