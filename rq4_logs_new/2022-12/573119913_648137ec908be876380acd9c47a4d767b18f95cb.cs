using Microsoft.Extensions.Configuration;
using VanKassa.Backend.Infrastructure.Data;
using VanKassa.Backend.Infrastructure.Data.Seeders;

namespace VanKassa.Backend.Core;

public class DatabaseManager
{
    private readonly VanKassaDbContext _dbContext;
    private readonly IConfiguration _configuration;

    public DatabaseManager(VanKassaDbContext dbContext, IConfiguration configuration)
    {
        _dbContext = dbContext;
        _configuration = configuration;
    }

    public void SeedDatabase()
    {
        SeedRoleTable();
        SeedUserTable();
        SeedOutletTable();
        SeedUserOutletTable();
    }

    private void SeedRoleTable()
    {
        var roleSeeder = new RoleSeeder(_dbContext, _configuration);
        
        roleSeeder.SeedIfEmpty();
    }

    private void SeedUserTable()
    {
        var userSeeder = new UserSeeder(_dbContext, _configuration);
        
        userSeeder.SeedIfEmpty();
    }

    private void SeedOutletTable()
    {
        var outletSeeder = new OutletSeeder(_dbContext, _configuration);
        
        outletSeeder.SeedIfEmpty();
    }

    private void SeedUserOutletTable()
    {
        var userOutletSeeder = new UserOutletSeeder(_dbContext, _configuration);
        
        userOutletSeeder.SeedIfEmpty();
    }
}