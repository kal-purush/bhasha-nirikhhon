using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace HospitalAdmin.Data
{
    public class DbInitializer
    {
        public static void Initialize(HospitalContext context)
        {
            context.Database.Migrate();

            if(!context.Hospitals.Any())
            {
                var hopsitals = SeedDb.GetHospitals();

                using(context)
                {
                    context.AddRange(hopsitals);
                    context.SaveChanges();
                }
            }
        }
    }
}