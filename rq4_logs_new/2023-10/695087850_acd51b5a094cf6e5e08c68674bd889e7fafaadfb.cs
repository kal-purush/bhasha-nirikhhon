using Microsoft.EntityFrameworkCore;

namespace FoodImportLabelGenerator.Data;

public class FoodImportLabelGeneratorContext : DbContext
{
    public DbSet<Label> Labels { get; set; }
    private readonly IConfiguration _configuration;

    public FoodImportLabelGeneratorContext(IConfiguration configuration)
    {
        _configuration = configuration;
    }
    
    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        var connectionString = _configuration.GetConnectionString("DefaultConnection");
        optionsBuilder.UseSqlServer(connectionString);
    }
}