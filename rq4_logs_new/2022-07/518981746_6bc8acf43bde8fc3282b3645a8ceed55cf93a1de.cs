using IntelliTect.Coalesce;
using IntelliTect.Coalesce.DataAnnotations;
using Microsoft.AspNetCore.DataProtection.EntityFrameworkCore;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using System.Security.Claims;
using UndergraduateResearchPortal.Data.Models;

namespace UndergraduateResearchPortal.Data;

[Coalesce]
public class AppDbContext : IdentityDbContext<User>, IDataProtectionKeyContext
{
    [InternalUse]
    public DbSet<DataProtectionKey> DataProtectionKeys { get; set; }
    public DbSet<Post> Posts { get; set; }
    public DbSet<Field> Fields { get; set; }
    public DbSet<Language> Languages { get; set; }
    public DbSet<Application> Applications { get; set; }

    public AppDbContext()
    {
    }

    public AppDbContext(DbContextOptions options) : base(options)
    {
    }

    protected override void OnModelCreating(ModelBuilder builder)
    {
        base.OnModelCreating(builder);

        // Remove cascading deletes.
        foreach (var relationship in builder.Model.GetEntityTypes().SelectMany(e => e.GetForeignKeys()))
        {
            relationship.DeleteBehavior = DeleteBehavior.Restrict;
        }
    }

    /// <summary>
    /// Migrates the database and sets up items that need to be set up from scratch.
    /// </summary>
    public void Initialize()
    {
        try
        {
            this.Database.Migrate();

            // TODO: Or, use Database.EnsureCreated() instead:
            // this.Database.EnsureCreated();
        }
        catch (InvalidOperationException e) when (e.Message == "No service for type 'Microsoft.EntityFrameworkCore.Migrations.IMigrator' has been registered.")
        {
            // this exception is expected when using an InMemory database
        }
    }
}