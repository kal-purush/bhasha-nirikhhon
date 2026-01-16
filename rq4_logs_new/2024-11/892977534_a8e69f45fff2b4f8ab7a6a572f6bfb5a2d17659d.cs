using Domain.Models;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.Databases
{
    public class RealDatabase : DbContext
    {
        public DbSet<Book> books { get; set; }
        public DbSet<Author> authors { get; set; }
        public DbSet<User> users { get; set; }

        public RealDatabase(DbContextOptions<RealDatabase> options) : base(options)
        {

        }
        
    }
}