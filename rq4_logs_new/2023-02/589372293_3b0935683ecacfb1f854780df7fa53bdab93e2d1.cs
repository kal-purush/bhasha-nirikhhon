using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;

namespace DomainDrivenDesign.List5_7;

public class DatabaseContext : DbContext
{
    private string _pathName;

    public DbSet<UserDataModel> Users { get; set; }

    public DatabaseContext(string pathName)
    {
        _pathName = pathName;
    }

    protected override void OnConfiguring(DbContextOptionsBuilder optionBuilder)
    {
        var connectionString = new SqliteConnectionStringBuilder { DataSource = @$"{_pathName}" }.ToString();
        optionBuilder.UseSqlite(new SqliteConnection(connectionString));
    }
}