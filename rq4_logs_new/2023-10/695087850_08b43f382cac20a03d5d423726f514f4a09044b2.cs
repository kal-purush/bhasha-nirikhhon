using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace FoodImportLabelGenerator.Data;

public class UsersContext : IdentityUserContext<IdentityUser>
{
    private readonly IConfiguration _configuration;
    
    public UsersContext (DbContextOptions<UsersContext> options, IConfiguration configuration)
        : base(options)
    {
        _configuration = configuration;
    }
    protected override void OnConfiguring(DbContextOptionsBuilder options)
    {
        var connectionString = _configuration["ConnectionStrings:DefaultConnection"];
        options.UseSqlServer(connectionString!);

    }
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
    }
}