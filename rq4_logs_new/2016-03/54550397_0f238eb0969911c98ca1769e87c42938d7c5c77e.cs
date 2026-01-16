using IdeasRepository.DAL.Entities;
using IdeasRepository.DAL.Initializers;
using Microsoft.AspNet.Identity.EntityFramework;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IdeasRepository.DAL.Contexts
{
    public class ApplicationDbContext : IdentityDbContext<ApplicationUser>
    {
        public ApplicationDbContext()
            : base("IdeasRepository", throwIfV1Schema: false)
        { }

        public DbSet<Record> Records { get; set; }
        public DbSet<RecordType> RecordTypes { get; set; }

        static ApplicationDbContext()
        {
            Database.SetInitializer(new ApplicationDbInitializer());
        }

        public static ApplicationDbContext Create()
        {
            return new ApplicationDbContext();
        }
    }
}