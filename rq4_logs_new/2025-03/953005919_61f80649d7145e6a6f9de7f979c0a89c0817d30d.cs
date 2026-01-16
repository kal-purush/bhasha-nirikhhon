using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using subbedsub.Models;

namespace subbedsub.Data
{
    public class AppDbContext : DbContext
    {
        public AppDbContext (DbContextOptions<AppDbContext> options)
            : base(options)
        {
        }
        public DbSet<OpenAIKey> OpenAIKeys { get; set; }
        public DbSet<UserPreference> UserPreferences { get; set; }
        
        protected override void OnConfiguring(DbContextOptionsBuilder options)
            => options.UseSqlite("Data Source=app_data.db");
    }
}