using Microsoft.EntityFrameworkCore;
using TestRemakeServer.DbModels;


namespace TestRemakeServer
{
    public class ClockProilesContext : DbContext
    {
        public DbSet<ClockProfile> ClockProiles { get; set; }

        public ClockProilesContext(DbContextOptions<ClockProilesContext> options)
            : base(options)
        { }
    }
}