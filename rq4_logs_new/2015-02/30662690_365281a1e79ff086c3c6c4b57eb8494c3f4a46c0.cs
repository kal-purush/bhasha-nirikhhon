using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Entity;

namespace ApresentacaoALimpo.Model
{
    public class MyDbContext : DbContext
    {
        public MyDbContext(string currentDb)
            : base(currentDb)
        {
            Database.SetInitializer(new DatabaseCreator());
        }

        public DbSet<Student> Students { get; set; }
        public DbSet<Asset> Assets { get; set; }
        public DbSet<Enrollment> Enrollments { get; set; }
    }

    public class Enrollment
    {
        public int Id { get; set; }
        public DateTime StartDate { get; set; }
        public DateTime ExitDate { get; set; }
        public Section Section { get; set; }        
    }

    public class Section
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }

    public class Asset
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public DateTime? Checkin { get; set; }
        public DateTime? Checkout { get; set; }
        public Student WithStudent { get; set; }
    }

    public class Student
    {
        public int Id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Email { get; set; }
        public virtual ICollection<Enrollment> Enrollments { get; set; }
    }

    class DatabaseCreator : DropCreateDatabaseIfModelChanges<MyDbContext>
    {
        protected override void Seed(MyDbContext context)
        {
            SeedStudents(context.Students);
            base.Seed(context);
        }

        private void SeedStudents(DbSet<Student> students)
        {
            students.Add(new Student { FirstName = "Mauricio", LastName = "Lobo", Email = "mau@lobo.com" });
            students.Add(new Student { FirstName = "Mayara", LastName = "Sa", Email = "mayara@sa.com" });
        }
    }
}