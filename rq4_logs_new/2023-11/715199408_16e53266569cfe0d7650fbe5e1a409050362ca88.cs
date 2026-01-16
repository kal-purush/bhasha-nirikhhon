using Microsoft.EntityFrameworkCore;
using NerdStore.Catalog.Domain.Models;
using NerdStore.Core.Data;

namespace NerdStore.Catalog.Data;

public class CatalogContext : DbContext, IUnitOfWork
{
    public CatalogContext(DbContextOptions<CatalogContext> options) : base(options) { }

    public DbSet<Product> Products { get; set; }
    public DbSet<Category> Categories { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        foreach (var property in modelBuilder.Model
            .GetEntityTypes()
            .SelectMany(e => e.GetProperties()
                .Where(p => p.ClrType == typeof(string))))
            property.SetColumnType("varchar(100)");

        modelBuilder.ApplyConfigurationsFromAssembly(typeof(CatalogContext).Assembly);
    }

    public async Task<bool> Commit()
    {
        foreach (var entry in ChangeTracker
            .Entries()
            .Where(entry => entry.Entity.GetType().GetProperty("RegistrationDate") != null))
        {
            if (entry.State == EntityState.Added)
                entry.Property("RegistrationDate").CurrentValue = DateTime.Now;

            if (entry.State == EntityState.Modified)
                entry.Property("RegistrationDate").IsModified = false;
        }

        return await base.SaveChangesAsync() > 0;
    }
}