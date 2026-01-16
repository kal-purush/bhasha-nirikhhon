using Microsoft.EntityFrameworkCore;
using AsxDbScraper.Models;

namespace AsxDbScraper.Data;

public class AsxDbContext : DbContext
{
    public AsxDbContext(DbContextOptions<AsxDbContext> options) : base(options)
    {
    }

    public DbSet<BalanceSheet> BalanceSheets { get; set; } = null!;
    public DbSet<IncomeStatement> IncomeStatements { get; set; } = null!;
    public DbSet<CashFlowStatement> CashFlowStatements { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<BalanceSheet>()
            .HasIndex(b => new { b.CompanyCode, b.StatementDate })
            .IsUnique();

        modelBuilder.Entity<IncomeStatement>()
            .HasIndex(i => new { i.CompanyCode, i.StatementDate })
            .IsUnique();

        modelBuilder.Entity<CashFlowStatement>()
            .HasIndex(c => new { c.CompanyCode, c.StatementDate })
            .IsUnique();
    }
} 