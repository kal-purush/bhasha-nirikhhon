using System.Reflection;
using Microsoft.EntityFrameworkCore;

namespace Codend.Persistence;

public sealed class CodendApplicationDbContext : DbContext
{
    public CodendApplicationDbContext()
    {
    }

    public CodendApplicationDbContext(DbContextOptions options) : base(options)
    {
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseSqlServer();
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfigurationsFromAssembly(Assembly.GetExecutingAssembly());

        base.OnModelCreating(modelBuilder);
    }
}