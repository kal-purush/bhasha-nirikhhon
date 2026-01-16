using LeedCodeDatabase.Models;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using System;

namespace LeedCodeDatabase
{
    public class DatabaseContext : DbContext
    {
        public DatabaseContext()
        {
            Database.EnsureCreated();
            Database.Migrate();          
        }

        public DbSet<Beer> Beers { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder
           optionsBuilder)
        {
            string connectionStringBuilder = new
               SqliteConnectionStringBuilder()
            {
                DataSource = "beer.db"
            }
            .ToString();

            var connection = new SqliteConnection(connectionStringBuilder);
            optionsBuilder.UseSqlite(connection);
        }

       
          
    }

}