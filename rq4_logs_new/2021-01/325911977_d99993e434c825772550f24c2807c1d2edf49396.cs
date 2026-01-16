using System.Reflection;
using Hahn.ApplicatonProcess.December2020.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Hahn.ApplicatonProcess.December2020.Data
{
    /// <summary>
    ///     Data context of the application
    /// </summary>

    public class ApplicationDBContext: DbContext
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ApplicationDBContext"/> class.
        /// </summary>
        /// <param name="options">DbContext options applied the class</param>

        public ApplicationDBContext(DbContextOptions<ApplicationDBContext> options) : base(options)
        { }

        /// <summary>
        /// Overrides the <see cref="OnModelCreating"/> method.
        /// </summary>
        /// <param name="modelBuilder"> defines the shape of the entities and how they map to the database</param>

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Applicant>().HasData(
            new Applicant
            {
                Id = 1,
                Name = "Ammie",
                FamilyName = "Patrick",
                Address = "Lagos, Nigeria",
                EmailAddress = "patty@me.com",
                CountryOforigin = "Nigeria",
                Age = 23,
                Hired = true
            }
        );
            modelBuilder.ApplyConfigurationsFromAssembly(Assembly.GetExecutingAssembly());
        }
    }
}