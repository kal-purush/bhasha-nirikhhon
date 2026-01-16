using DataAccessLayer.Entities;
using Microsoft.EntityFrameworkCore;
using System;

namespace DataAccessLayer.Contexts
{
    public class EFCoreDBContext : DbContext
    {
        public DbSet<Point> Locations { get; set; }
        public DbSet<Book> Books { get; set; }
        public DbSet<Library> Libraries { get; set; }
        public DbSet<City> Cities { get; set; }
        public DbSet<Client> Clients { get; set; }
        public DbSet<BookRevision> BookRevisions { get; set; }
        public DbSet<LibraryBook> LibraryBooks { get; set; }
        public DbSet<RentBook> RentBooks { get; set; }

        public EFCoreDBContext(DbContextOptions<EFCoreDBContext> options) : base(options)
        {
            Database.EnsureCreated();
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {

        }
    }
}