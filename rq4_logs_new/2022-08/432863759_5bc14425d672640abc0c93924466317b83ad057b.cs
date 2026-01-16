using Data;
using Data.Constants;
using Data.Entities;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test.Fixtures
{
    public class VolunteerDalContextFixture : IDisposable
    {
        public readonly AppDbContext context;
        public readonly Guid userKey = Guid.NewGuid();
        public VolunteerDalContextFixture()
        {
            var optionsBuilder = new DbContextOptionsBuilder<AppDbContext>();
            optionsBuilder.UseInMemoryDatabase("Test");
            context = new AppDbContext(optionsBuilder.Options);
            // Data seed
            var data = new List<Volunteer>
            {
                new Volunteer { Id = 1, Address = "Address1", Status = VolunteerStatus.Induction, PostCode = "WN3 4TB", Email = "volunteer1@gmail.com", FirstName = "Volunteer1", Key = Guid.NewGuid()},
                new Volunteer { Id = 2, Address = "Address2", Status = VolunteerStatus.DBS, PostCode = "WN3 4ZB", Email = "volunteer2@gmail.com", FirstName = "Volunteer2", Key = userKey},
                new Volunteer { Id = 3, Address = "Address3", Status = VolunteerStatus.Completed, PostCode = "WN6 4TB", Email = "volunteer3@gmail.com", FirstName = "Volunteer3", Key = new() },
                new Volunteer { Id = 4, Address = "Address4", Status = VolunteerStatus.Completed, PostCode = "WN1 4TB", Email = "volunteer4@gmail.com", FirstName = "Volunteer4", Key = new() },
            };

            context.Volunteers.AddRange(data);
            context.SaveChanges();
        }
        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }
    }
}