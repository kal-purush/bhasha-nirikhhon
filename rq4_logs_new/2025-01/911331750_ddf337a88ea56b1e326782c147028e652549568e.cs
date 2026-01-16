using Microsoft.EntityFrameworkCore;
using AppVecinos.API.Models;

namespace AppVecinos.API.Data
{
    public class DataContext : DbContext
    {
        public DataContext(DbContextOptions<DataContext> options) : base(options) { }

        public DbSet<Neighbor> Neighbors { get; set; }

        public DbSet<Fee> Fees { get; set; }
        
        public DbSet<Payment> Payments { get; set; }
        public DbSet<Balance> Balances { get; set; }
        
        public DbSet<Outcome> Outcomes { get; set; }  
    }
}