using EFCoreExercise.Models;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EFCoreExercise
{
    public class DataContext : DbContext
    {
        public DataContext(DbContextOptions options) : base(options)
        {

        }
        public DataContext()
        {

        }
        public DbSet<Class> Class { get; set; }
        public DbSet<Student> Student { get; set; }

    }
}