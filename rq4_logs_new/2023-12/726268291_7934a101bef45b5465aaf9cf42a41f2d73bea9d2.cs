using LT.DigitalOffice.DashboardService.Models.Db;
using LT.DigitalOffice.Kernel.EFSupport.Provider;
using Microsoft.EntityFrameworkCore;
using System.Reflection;
using System.Threading.Tasks;

namespace LT.DigitalOffice.DashboardService.Data.Provider.MsSql.Ef;

public class DashboardServiceDbContext : DbContext, IDataProvider
{
  public DbSet<DbBoard> Boards { get; set; }
  public DbSet<DbChangeLog> ChangeLogs { get; set; }
  public DbSet<DbComment> Comments { get; set; }
  public DbSet<DbGroup> Groups { get; set; }
  public DbSet<DbPriority> Priorities { get; set; }
  public DbSet<DbTask> Tasks { get; set; }
  public DbSet<DbTaskType> TaskTypes { get; set; }

  public DashboardServiceDbContext(DbContextOptions<DashboardServiceDbContext> options) : base(options) { }

  protected override void OnModelCreating(ModelBuilder modelBuilder)
  {
    modelBuilder.ApplyConfigurationsFromAssembly(Assembly.Load("LT.DigitalOffice.DashboardService.Models.Db"));
  }

  public object MakeEntityDetached(object obj)
  {
    Entry(obj).State = EntityState.Detached;
    return Entry(obj).State;
  }

  public void EnsureDeleted()
  {
    Database.EnsureDeleted();
  }

  public bool IsInMemory()
  {
    return Database.IsInMemory();
  }

  public void Save()
  {
    SaveChanges();
  }

  async Task IBaseDataProvider.SaveAsync()
  {
    await SaveChangesAsync();
  }
}