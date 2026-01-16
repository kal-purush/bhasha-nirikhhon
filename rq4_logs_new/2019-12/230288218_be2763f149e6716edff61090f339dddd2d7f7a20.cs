using Domain;
using Microsoft.EntityFrameworkCore;
using System;

namespace DataAccess
{
    public class Context : DbContext
    {
        public Context() 
        {
            Database.EnsureCreated();
        }
        public DbSet <MyFile> Files { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlServer("Server=A-104-13;Database=FilesApp;Trusted_Connection=True;");
        }
    }
}