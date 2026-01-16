using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Domain.Entities;
using Domain.ValueObjects;
using Persistence.PersistenceFolder;

namespace Persistence
{
    public static class DbSeeder
    {
        public static void Seed(DatabaseContext dbContext)
        {
            // Create default User (if there are none)
            if (!dbContext.Users.Any())
            {
                CreateAdminUser(dbContext);
            }

            // Create default MeansOfTransport
            if (!dbContext.MeansOfTransport.Any())
            {
                CreateMeansOfTransport(dbContext);
            }
        }

        private static async Task CreateAdminUser(DatabaseContext dbContext)
        {
            // Create the default user account
            var userAdmin = User.Create("admin", "admin", "admin", "admin@admin.com", 5, new List<Comment>());

            dbContext.Add(userAdmin);

            dbContext.SaveChangesAsync();
        }

        private static void CreateMeansOfTransport(DatabaseContext dbContext)
        {
            // IS - for trams
            var num = 2;
            for (int i = 1; i <= num; i++)
            {
                CreateMeanOfTransport(dbContext, "IS12", i);
            }

            //BUS - for buses
            var num2 = 2;
            for (int i = 1; i <= num2; i++)
            {
                CreateMeanOfTransport(dbContext, "BUS12", i);
            }
            
            dbContext.SaveChanges();
        }


        private static void CreateMeanOfTransport(DatabaseContext dbContext, string prefix, int num)
        {
            var idefCode = prefix + num;
            Coordinates coord = new Coordinates(0.0, 0.0);
            var meanOfTransport = TransportMean.Create(idefCode, new List<Comment>(), 0, coord, num);

            dbContext.MeansOfTransport.Add(meanOfTransport);
            dbContext.SaveChanges();
        }
    }
}